defmodule OverDB.Protocol.V4.Frames.Requests.Execute do

  alias OverDB.Protocol.V4.Frames.{Responses.Result.Prepared, Requests.Encoder, Frame}

  @type t :: :execute


  def opcode() do
    0x0A
  end

  @spec new(Prepared.t, map) :: list
  def new(%Prepared{id: id, metadata: metadata, result_metadata: result_metadata, values: values}, opts) do
    flags = Map.get(opts, :flags, %{ignore: true})
    Frame.create(:execute, Encoder.execute_new_body(id, metadata, result_metadata, values, opts), flags) |> Frame.encode()
  end

  @spec push_alone(Prepared.t, map) :: list
  def push_alone(%Prepared{id: id, metadata: metadata, result_metadata: result_metadata, values: values}, opts) do
    flags = Map.get(opts, :flags, %{ignore: true})
    Frame.create(:execute, Encoder.execute_new_body(id, metadata, result_metadata, values, opts), flags) |> Frame.push()
  end

  @spec push(t, map) :: list
  def push(%Prepared{} = query, opts \\ %{}) do
    Encoder.query_in_batch(query, opts)
  end
end
