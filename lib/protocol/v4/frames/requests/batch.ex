defmodule OverDB.Protocol.V4.Frames.Requests.Batch do

  alias OverDB.Protocol.V4.Frames.{Requests.Encoder, Frame}
  @type t :: :batch

  def opcode() do
    0x0D
  end


  # NOTE: this fucntion only recv already encoded query_in_batch queries
  def push(queries, type, stream_id, opts \\ %{}) do
    flags = Map.get(opts, :flags, %{ignore: true})
    body = Encoder.batch_push_body(queries, type, opts)
    Frame.create(:batch, body, flags, stream_id) |> Frame.encode()
  end

  # NOTE: this fucntion recv plain(queries_list) non encoded queries! note:
  #  each query must be %Query{} or %Prepared{}.
  def new(queries, type, stream_id, opts \\ %{}) do
    flags = Map.get(opts, :flags, %{ignore: true})
    body = Encoder.batch_new_body(queries, type, opts)
    Frame.create(:batch, body, flags, stream_id) |> Frame.encode()
  end

end
