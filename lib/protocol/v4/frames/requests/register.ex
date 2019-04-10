defmodule OverDB.Protocol.V4.Frames.Requests.Register do
  
  alias OverDB.Protocol.V4.Frames.{Frame, Requests.Encoder}

  @type t :: :register

  def opcode() do
    0x0B
  end

  def new(events) when is_list(events) do
    Frame.create(:register, Encoder.string_list(events)) |> Frame.encode()
  end

end
