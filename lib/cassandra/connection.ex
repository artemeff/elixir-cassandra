defmodule Cassandra.Connection do
  use Connection

  require Logger

  alias :gen_tcp, as: TCP
  alias CQL.{Frame, Startup, Ready, Options, Query, QueryParams, Error, Prepare, Execute, Register, Event}
  alias CQL.Result.Rows

  @backoff_init 500
  @backoff_mult 1.6
  @backoff_jitt 0.2
  @backoff_max  12000

  @default_params %{
    consistency: :one,
    skip_metadata: false,
    page_size: 100,
    paging_state: nil,
    serial_consistency: nil,
    timestamp: nil,
  }

  @valid_params Map.keys(@default_params)

  # Client API

  def start_link(options \\ []) do
    Connection.start_link(__MODULE__, options)
  end

  def options(connection, timeout \\ :infinity) do
    Connection.call(connection, :options, timeout)
  end

  def use(connection, keyspace, timeout \\ :infinity) do
    Connection.call(connection, {:use, keyspace}, timeout)
  end

  def query(connection, query, values \\ [], options \\ [], timeout \\ :infinity) do
    Connection.call(connection, {:query, query, values, options}, timeout)
  end

  def prepare(connection, query, timeout \\ :infinity) do
    Connection.call(connection, {:prepare, query}, timeout)
  end

  def execute(connection, id, values \\ [], options \\ [], timeout \\ :infinity) do
    Connection.call(connection, {:execute, id, values, options}, timeout)
  end

  def register(connection, types, timeout \\ :infinity) do
    case Connection.call(connection, {:register, List.wrap(types)}, timeout) do
      %Ready{} ->
        {:ok, Connection.call(connection, :event_stream)}
      reason ->
        {:error, reason}
    end
  end

  def stop(connection) do
    GenServer.stop(connection)
  end

  # Connection Callbacks

  def init(options) do
    host     = Keyword.get(options, :hostname, "127.0.0.1") |> to_charlist
    port     = Keyword.get(options, :port, 9042)
    timeout  = Keyword.get(options, :timeout, 5000)
    keyspace = Keyword.get(options, :keyspace)

    {:ok, manager} = GenEvent.start_link

    state = %{
      host: host,
      port: port,
      timeout: timeout,
      waiting: [],
      streams: %{},
      last_stream_id: 0,
      socket: nil,
      backoff: next_backoff,
      keyspace: keyspace,
      event_manager: manager,
    }

    {:connect, :init, state}
  end

  def connect(_info, %{host: host, port: port, timeout: timeout} = state) do
    with {:ok, socket} <- TCP.connect(host, port, [:binary, active: false]),
         :ok <- handshake(socket, timeout)
      do
      :ok = send_use(state.keyspace, state.socket)
      {:ok, stream_all(%{state | socket: socket, backoff: next_backoff})}
    else
      :stop ->
        {:stop, :handshake_error, state}
      _ ->
        {:backoff, state.backoff, update_in(state.backoff, &next_backoff/1)}
    end
  end

  def disconnect(info, %{socket: socket} = state) do
    :ok = TCP.close(socket)
    case info do
      {:error, :closed} ->
        Logger.error("#{__MODULE__} Connection closed\n")
      {:error, reason} ->
        message = :inet.format_error(reason)
        Logger.error("#{__MODULE__} Connection error: #{message}")
    end
    new_state = %{state |
      waiting: Map.values(state.streams),
      streams: %{},
      last_stream_id: 0,
      socket: nil,
    }
    {:connect, :reconnect, new_state}
  end

  def terminate(reason, %{socket: socket} = state) do
    state.streams
    |> Map.values
    |> Enum.concat(state.waiting)
    |> Enum.each(fn {_, from} -> Connection.reply(from, :error) end)

    unless is_nil(socket), do: TCP.close(socket)
    Logger.error("Terminating #{__MODULE__}: #{inspect reason}")
  end

  def handle_call(:options, from, state) do
    {:noreply, stream(%Options{}, from, state)}
  end

  def handle_call({:query, query, values, options}, from, state) do
    request = %Query{
      query: query,
      params: params(values, options)
    }
    {:noreply, stream(request, from, state)}
  end

  def handle_call({:use, keyspace}, _from, state) do
    :ok = send_use(keyspace, state.socket)
    {:reply, :ok, %{state | keyspace: keyspace}}
  end

  def handle_call({:prepare, query}, from, state) do
    request = %Prepare{query: query}
    {:noreply, stream(request, from, state)}
  end

  def handle_call({:execute, id, values, options}, from, state) do
    request = %Execute{
      id: id,
      params: params(values, options)
    }
    {:noreply, stream(request, from, state)}
  end

  def handle_call({:register, types}, from, state) do
    request = %Register{types: types}
    {:noreply, stream(request, from, state)}
  end

  def handle_call(:event_stream, _from, %{event_manager: manager} = state) do
    {:reply, GenEvent.stream(manager), state}
  end

  def handle_info({:tcp, socket, buffer}, %{socket: socket} = state) do
    %Frame{stream: id} = frame = CQL.decode(buffer)
    case id do
      -1 -> handle_event(frame, state)
       0 -> {:noreply, state}
       _ -> handle_response(frame, state)
    end
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :closed}, state}
  end

  # Helpers

  defp handle_event(%Frame{body: %Event{} = event}, state) do
    GenEvent.ack_notify(state.event_manager, event)
    {:noreply, state}
  end

  defp handle_response(%Frame{stream: id, body: %Rows{data: data}}, state) do
    handle_response(%Frame{stream: id, body: data}, state)
  end

  defp handle_response(%Frame{stream: id, body: body}, state) do
    {{_, from}, new_state} = pop_in(state.streams[id])
    Connection.reply(from, body)
    {:noreply, new_state}
  end

  defp params(values, options) do
    params =
      options
      |> Keyword.take(@valid_params)
      |> Enum.into(@default_params)
      |> Map.put(:values, values)

    struct(QueryParams, params)
  end

  defp stream_all(state) do
    Enum.reduce state.waiting, state, fn
      ({request, from}, state) -> stream(request, from, state)
    end
  end

  defp stream(request, from, %{socket: nil} = state) do
    update_in(state.waiting, &[{request, from} | &1])
  end

  defp stream(request, from, %{socket: socket, last_stream_id: id} = state) do
    id = next_stream_id(id)
    stream_to(socket, request, id)

    state
    |> Map.put(:last_stream_id, id)
    |> put_in([:streams, id], {request, from})
  end

  defp stream_to(socket, request, id) do
    request
    |> CQL.encode(id)
    |> send_to(socket)
  end

  defp send_to(request, socket) do
    TCP.send(socket, request)
  end

  defp send_startup(socket) do
    %Startup{}
    |> CQL.encode
    |> send_to(socket)
  end

  defp handshake(socket, timeout) do
    with :ok <- send_startup(socket),
         {:ok, buffer} <- TCP.recv(socket, 0, timeout),
         %Frame{body: %Ready{}} <- CQL.decode(buffer)
      do
        :inet.setopts(socket, [active: true])
    else
      %Frame{body: %Error{message: message}} ->
        Logger.error("Handshake error: #{message}")
        :stop
      {:error, :closed} ->
        Logger.error("Connection closed before handshake")
        :error
      {:error, reason} ->
        message = :inet.format_error(reason)
        Logger.error("Handshake error: #{message}")
        :error
      error ->
        Logger.error("Handshake error: #{inspect error}")
        :error
    end
  end

  defp send_use(nil, _), do: :ok
  defp send_use(_, nil), do: :ok
  defp send_use(keyspace, socket) do
    %Query{
      query: "USE '#{keyspace}';",
      params: params([], [])
    }
    |> CQL.encode
    |> send_to(socket)
  end

  defp next_backoff(current \\ @backoff_init) do
    next = current * @backoff_mult
    jitt = (:rand.uniform - 0.5) * @backoff_jitt * current
    round(min(next, @backoff_max) + jitt)
  end

  defp next_stream_id(32768), do: 1
  defp next_stream_id(n), do: n + 1
end