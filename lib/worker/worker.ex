defmodule OverDB.Worker do

  defmacro __using__(opts) do
    quote do
      alias unquote(opts[:executor]), as: Execute
      alias OverDB.Protocol
      alias OverDB.Protocol.V4.Frames.Responses.Result.{Ignore, Rows, Compute, SchemaChange, SetKeyspace, Void}
      alias OverDB.Protocol.V4.Frames.Responses.{Error, Event}
      import OverDB.Builder.Query
    end
  end

end
