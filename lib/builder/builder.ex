defmodule OverDB.Builder do

  def build(type, modules, value) do
    quote do
      Map.put(OverDB.Builder.query_map(unquote(modules)), unquote(type), unquote(value))
    end
  end

  def query_map(modules) do
    case modules do
      {keyspace, table} -> %{
        target: "#{keyspace.__struct__.name}.#{table.__struct__.name}",
        opts: %{},
        ref: :rand.uniform(9223372036854775807),
        send_cast?: :cast,
        keyspace: keyspace,
        response_to: self(),
        prepare?: true,
        schema: table,
        rf: keyspace.__struct__.rf
      }
      _ -> modules
    end
  end
end
