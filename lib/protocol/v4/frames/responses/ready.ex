defmodule OverDB.Protocol.V4.Frames.Responses.Ready do

  import OverDB.Protocol.V4.Frames.Responses.Decoder, only: [ready: 1]
  alias OverDB.Protocol.V4.Frames.Frame

  @type t :: :ready

  def opcode() do
    0x02
  end

  @spec decode(Frame.t) :: t
  def decode(%Frame{opcode: :ready, body: body}) do
    ready(body)
  end



end
