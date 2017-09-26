defmodule Cassandra.Host do
  @moduledoc """
  Represents a Cassandra host
  """

  defstruct [
    :ip,
    :id,
    :data_center,
    :rack,
    :distance,
    :release_version,
    :schema_version,
    :tokens,
    :status,
  ]

  @doc """
  Creates a new Host struct from given data came from Cassandra `system.peers` or `system.local` tabels
  """
  def new(data, status \\ :down, data_center \\ nil, parser \\ fn x -> x end) do
    with {:ok, ip}   <- peer_ip(data),
         {:ok, host} <- from_data(data, parser),
         distance    <- distance(host, data_center)
    do
      status = case status do
        :up   -> :up
        :down -> :down
        _     -> ping(ip)
      end
      %{host | ip: ip, status: status, distance: distance}
    else
      :error -> nil
    end
  end

  def distance(host, data_center)
  def distance(%__MODULE__{data_center: _}, nil), do: :ignore
  def distance(%__MODULE__{data_center: x}, x),   do: :local
  def distance(%__MODULE__{data_center: _}, _),   do: :remote

  @doc """
  Chacks whether the `host` status is up or not
  """
  def up?(host)
  def up?({_, host}),            do: up?(host)
  def up?(%__MODULE__{} = host), do: host.status == :up

  @doc """
  Chacks whether the `host` status is down or not
  """
  def down?(host)
  def down?({_, host}),            do: down?(host)
  def down?(%__MODULE__{} = host), do: host.status == :down

  @doc """
  Toggles the `host` status to `status`.
  When `status` is `:get` it tries to get host status in network
  """
  def toggle(%__MODULE__{} = host, status)
  when status == :up or status == :down do
    %{host | status: status}
  end

  def toggle(%__MODULE__{ip: ip} = host, :get) do
    %{host | status: ping(ip)}
  end

  ### Helpers ###

  defp peer_ip(%{"rpc_address" => {0, 0, 0, 0}, "peer" => peer}), do: {:ok, peer}
  defp peer_ip(%{"rpc_address" => nil, "peer" => peer}), do: {:ok, peer}
  defp peer_ip(%{"rpc_address" => ip}) when not is_nil(ip), do: {:ok, ip}
  defp peer_ip(%{"broadcast_address" => ip}) when not is_nil(ip), do: {:ok, ip}
  defp peer_ip(_), do: :error

  defp from_data(%{
    "host_id" => id,
    "data_center" => data_center,
    "rack" => rack,
    "release_version" => release_version,
    "schema_version" => schema_version,
    "tokens" => tokens,
  }, parser)
  do
    host = %__MODULE__{
      id: id,
      data_center: data_center,
      rack: rack,
      release_version: release_version,
      schema_version: schema_version,
      tokens: Enum.map(tokens, parser),
    }
    {:ok, host}
  end
  defp from_data(_, _), do: :error

  defp ping(%__MODULE__{ip: ip}), do: ping(ip)
  defp ping(ip, port \\ 9042, timeout \\ 100) do
    case :gen_tcp.connect(ip, port, [active: false], timeout) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        :up
      _ ->
        :down
    end
  end
end
