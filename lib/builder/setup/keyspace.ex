defmodule OverDB.Builder.Setup.Keyspace do

  alias OverDB.Protocol.V4.Frames.Requests.Query
  alias OverDB.{Protocol, Connection}
  require Logger

  # TODO: creating the keyspace with quorum enabled, to make sure all the nodes got it.
  # same apply to table_setup.
  def setup(keyspace, otp_apps) do
    cql = build(keyspace)
    for otp_app <- otp_apps do
      dcs = Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__]
      if dcs do
        {_, nodes} = dcs |> Enum.random()
        {address , port} = Enum.random(nodes)
        case Connection.start(%{address: address, port: port, shard: 0, strategy: :sync}) do
          %Connection{socket: socket} ->
            response = Query.create(cql, [])
            |> Query.new()
            |> Connection.push(socket)
            |> Connection.sync_recv()
            |> Protocol.decode_frame(%{})
            Logger.info("keyspace: #{inspect response}")
            :gen_tcp.close(socket)
          _ ->
           raise "Make sure scylla cluster is alive at compilation to init the required keyspace"
        end
      else
        Logger.error("Could not create keyspace: #{keyspace}
          as no overdb configuration has been found for the following app: #{otp_app}.
          Please make sure to recompile the project again.")
      end
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
