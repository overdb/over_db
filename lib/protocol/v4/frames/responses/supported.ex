defmodule OverDB.Protocol.V4.Frames.Responses.Supported do

  alias OverDB.Protocol.V4.Frames.Frame
  alias OverDB.Protocol.V4.Frames.Responses.Decoder

  def opcode() do
    0x06
  end

  @spec decode(Frame.t) :: map
  def decode(%Frame{opcode: :supported, body: body}= _frame) do
    {multi_map, <<>>} = Decoder.string_multimap(body)
    multi_map
  end


end
