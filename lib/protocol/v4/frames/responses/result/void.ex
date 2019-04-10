defmodule OverDB.Protocol.V4.Frames.Responses.Result.Void do

  @type t :: :void

  @spec create() :: t
  def create() do
    :void
  end

end
