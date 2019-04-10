defmodule OverDB.Protocol.V4.Frames.Requests.Prepare do
  alias OverDB.Protocol.V4.Frames.Frame

  @type t :: :prepare

  def opcode() do
    0x09
  end

  def new(statement) do
    Frame.create(:prepare, [<<byte_size(statement)::32>>, statement]) |> Frame.encode()
  end

  def push(statement) do
    Frame.create(:prepare, [<<byte_size(statement)::32>>, statement]) |> Frame.push()
  end

end
