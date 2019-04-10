defmodule OverDB.Builder.Table do
  defmacro __using__(opts) do
    keyspaces = opts[:keyspaces]
    quotes =
      Enum.map(keyspaces, fn {keyspace, _} ->
        quote do
          require unquote(keyspace)
        end
      end)
    Process.sleep(5000)
    [
      quote do
        import OverDB.Builder.Table
        @keyspaces unquote(keyspaces)
        require OverDB.Builder.Table
      end | quotes
    ]
  end

  defmacro table(name, type \\ :normal, [do: block]) do
    quote do
      @after_compile __MODULE__

      @table []
      @columns Keyword.new

      unquote(block)

      Module.put_attribute(__MODULE__, :table, [
        { :name, unquote(name) },
        { :type, unquote(type) },
        { :columns, Enum.reverse(Module.get_attribute(__MODULE__, :columns)) }
        | Module.get_attribute(__MODULE__, :table)
      ])

      def __after_compile__(_, _), do: OverDB.Builder.Setup.Table.setup(__MODULE__.__struct__, @keyspaces)

      defstruct Module.get_attribute(__MODULE__, :table)
    end
  end

  defmacro column(name, type, opts \\ []) do
    quote do
      columns = Module.get_attribute(__MODULE__, :columns)
        |> Keyword.put(unquote(name), %{
          type: unquote(type),
          opts: unquote(opts)
        })
      Module.put_attribute(__MODULE__, :columns, columns)
    end
  end

  defmacro partition_key(keys) do
    quote do
      Module.put_attribute(__MODULE__, :table, [
        { :partition_key, unquote(keys) }
        | Module.get_attribute(__MODULE__, :table)
      ])
    end
  end

  defmacro cluster_columns(cluster_columns) do
    quote do
      Module.put_attribute(__MODULE__, :table, [
        { :cluster_columns, unquote(cluster_columns) }
        | Module.get_attribute(__MODULE__, :table)
      ])
    end
  end

  defmacro with_options(opts \\ []) do
    quote do
      Module.put_attribute(__MODULE__, :table, [
        { :with_options, unquote(opts) }
        | Module.get_attribute(__MODULE__, :table)
      ])
    end
  end
end
