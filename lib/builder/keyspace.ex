defmodule OverDB.Builder.Keyspace do
  defmacro __using__(opts) do
    quote do
      import OverDB.Builder.Keyspace
      @otp_apps unquote(opts[:otp_apps])
      require OverDB.Builder.Keyspace
    end
  end


  defmacro keyspace(name, [do: block]) do
    quote do
      @after_compile __MODULE__

      @keyspace []

      unquote(block)

      Module.put_attribute(__MODULE__, :keyspace, [
        { :name, unquote(name) }
        | Module.get_attribute(__MODULE__, :keyspace)
      ])

      def __after_compile__(_, _), do: OverDB.Builder.Setup.Keyspace.setup(__MODULE__.__struct__, @otp_apps)

      defstruct Module.get_attribute(__MODULE__, :keyspace)
    end
  end

  defmacro with_options(opts) do
    quote do
      Module.put_attribute(__MODULE__, :keyspace, [
        { :with_options, unquote(opts) }
        | Module.get_attribute(__MODULE__, :keyspace)
      ])
    end
  end

  defmacro replication_factor(rf) do
    quote do
      Module.put_attribute(__MODULE__, :keyspace, [
        { :rf, unquote(rf) }
        | Module.get_attribute(__MODULE__, :keyspace)
      ])
    end
  end


end
