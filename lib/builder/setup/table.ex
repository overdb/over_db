defmodule OverDB.Builder.Setup.Table do

  alias OverDB.Protocol.V4.Frames.Requests.Query
  alias OverDB.{Protocol, Connection}
  require Logger

  # TODO: quorum will be appreciated.
  def setup(schema, keyspaces) do
    for {keyspace, otp_apps} <- keyspaces  do
      cql = build(schema, keyspace)
      Enum.each(otp_apps, fn(otp_app) ->
        dcs = Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__]
        if dcs do
          {_, nodes} = dcs |> Enum.random()
          {address , port} = Enum.random(nodes)
          %Connection{socket: socket} = Connection.start(%{address: address, port: port, shard: 0, strategy: :sync})
          response = Query.create(cql, [])
          |> Query.new()
          |> Connection.push(socket)
          |> Connection.sync_recv()
          |> Protocol.decode_frame(%{})
          Logger.info("table: #{inspect response}")
          :gen_tcp.close(socket)
        else
          Logger.error("Could not create table in the given keypsace: #{keyspace}
            as no overdb configuration has been found for the following app: #{otp_app}.
            Please make sure to recompile the project again.")
        end
       end)
    end
  end

  defp build(schema, keyspace) do
    keyspace_name = keyspace.__struct__.name
    target = "#{keyspace_name}.#{schema.name}"
    with_options = Map.get(schema, :with_options)
    table(target) <> "(" <> columns(schema.columns) <>
    primary_key(schema.partition_key, schema.cluster_columns) <> ")"
    <> with_options(with_options)
  end

  defp table(name), do: "CREATE TABLE IF NOT EXISTS #{name}"

  defp columns(columns), do: columns |> Enum.map(fn column -> column(column) end) |> Enum.join(", ")

  defp column({column, %{type: type}}), do: "#{column} #{type(type)}"


  defp type({:map, k_type, v_type}) do
    "map<#{type(k_type)},#{type(v_type)}>"
  end

  defp type({:tuple, types}) when is_list(types) do
    types = types |> Enum.map(fn type -> type(type) end) |> Enum.join(", ")
    "tuple<#{types}>"
  end

  defp type({set_list, e_type}) do
    "#{set_list}<#{type(e_type)}>"
  end

  defp type(type) when is_atom(type) do
    "#{type}"
  end

  defp with_options(opts) when is_list(opts) do
    cql = opts |> Enum.map(fn opt -> with_option(opt) end) |> Enum.join(" AND ")
    " WITH " <> cql
  end

  defp with_options(_), do: ""

  defp with_option({:clustering_order_by, orders}) do
    orders = orders |> Enum.map(fn {column, order} -> "#{column} #{order}" end) |> Enum.join(", ")
    "CLUSTERING ORDER BY (" <> orders <> ")"
  end

  defp with_option({opt, value}), do: "#{String.upcase(to_string(opt))} = #{value}"

  defp primary_key(partition_key, nil) when is_list(partition_key) do
    ", PRIMARY KEY((" <> Enum.join(partition_key, ", ") <> "))"
  end

  defp primary_key(partition_key, cluster_columns) when is_list(partition_key) and is_list(cluster_columns) do
    ", PRIMARY KEY((" <> Enum.join(partition_key, ", ") <> "), #{Enum.join(cluster_columns, ", ")})"
  end

  defp primary_key(_, _), do: ""





end
