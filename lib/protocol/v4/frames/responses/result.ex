defmodule OverDB.Protocol.V4.Frames.Responses.Result do

  alias OverDB.Protocol.V4.Frames.Frame
  alias OverDB.Protocol.V4.Frames.Responses.Result.Prepared

  import OverDB.Protocol.V4.Frames.Responses.Decoder, only: [result: 2, result: 3]

  @type t :: Void.t | Rows.t | Prepared.t | SetKeyspace.t | SchemaChange.t

  def opcode() do
    0x08
  end

  @spec decode(Frame.t, map) :: Void.t | Rows.t | Prepared.t | SetKeyspace.t | SchemaChange.t
  def decode(%Frame{opcode: :result, body: body}, opts) do
    result(body, opts)
  end

  @spec decode(Frame.t, Prepared.t) :: Rows.t
  def decode(%Frame{opcode: :result, body: body}, prepared, opts) do
    result(body, prepared, opts)
  end



end
