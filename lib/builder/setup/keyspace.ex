defmodule OverDB.Builder.Setup.Keyspace do

  alias OverDB.Protocol.V4.Frames.Requests.Query
  alias OverDB.Connection
  alias OverDB.Protocol
  require Logger

  # TODO: creating the keyspace with quorum enabled, to make sure all the nodes got it.
  def setup(keyspace, otp_apps) do
    cql = build(keyspace)
    for otp_app <- otp_apps do
      {_, nodes} = Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__] |> Enum.random()
      {address , port} = Enum.random(nodes)
      %Connection{socket: socket} = Connection.start(%{address: address, port: port, shard: 0, strategy: :sync})
      Query.create(cql, [])
      |> Query.new()
      |> Connection.push(socket)
      response = Protocol.decode_response(socket, %{})
      Logger.info("keyspace: #{inspect response}")
      :gen_tcp.close(socket)
    end
  end

  defp build(keyspace) do
    keyspace(keyspace.name) <>
    with_options(keyspace.with_options)
  end

  defp keyspace(name), do: "CREATE KEYSPACE IF NOT EXISTS #{name}"

  defp with_options(opts) when is_list(opts) do
    cql = opts |> Enum.map(fn opt -> with_option(opt) end) |> Enum.join(" AND ")
    " WITH " <> cql
  end

  defp with_options(_), do: ""

  defp with_option({opt, value}), do: "#{String.upcase(to_string(opt))} = #{value}"
end