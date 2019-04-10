defmodule OverDB.Protocol.V4.Frames.Requests.Options do
  require Logger

  alias OverDB.Protocol.V4.Frames.Frame

  @type t :: :options


  def opcode() do
    0x05
  end

  @spec new() :: list
  def new() do
    Frame.create(:options) |> encode() |> Frame.encode()
  end

  # NOTE: We will start working on the options request as soon as we setup our connection managment
  @spec encode(Frame.t) :: Frame.t
  def encode(%Frame{opcode: :options, body: [], flags: %{ignore: true}} = frame) do
    frame
  end

  @spec encode(Frame.t) :: :ok
  def encode(%Frame{opcode: :options, body: _, flags: _}) do
    Logger.error("You can't have flags otherthan %{ignore: true}, and empty list [] as body for options request")
  end


end
