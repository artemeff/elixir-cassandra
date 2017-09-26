defmodule Cassandra.ConnectionError do
  defexception [:host, :port, :action, :reason]

  def new(action, reason) do
    new(nil, nil, action, reason)
  end

  def new(host, action, reason) do
    new(host, 9042, action, reason)
  end

  def new(host, port, action, reason) do
    struct(__MODULE__, [host: host, port: port, action: action, reason: reason])
  end

  def message(%__MODULE__{host: nil, port: nil, action: action, reason: reason}) do
    "#{action} #{format(reason)}"
  end
  def message(%__MODULE__{host: host, port: port, action: action, reason: reason}) do
    "#{format_host_port(host, port)} #{action} #{format(reason)}"
  end

  defp format(reason) when is_atom(reason) do
    case :inet.format_error(reason) do
      'unknown POSIX error' -> inspect(reason)
      reason                -> String.Chars.to_string(reason)
    end
  end
  defp format(reason) when is_binary(reason), do: reason

  defp format_host_port(host, port) when is_binary(host) or is_list(host), do: "#{host}:#{port}"
  defp format_host_port(host, port) when is_tuple(host), do: "#{:inet_parse.ntoa(host)}:#{port}"
  defp format_host_port(host, port), do: "#{inspect(host)} (#{port})"
end
