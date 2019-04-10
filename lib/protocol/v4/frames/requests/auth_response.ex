defmodule OverDB.Protocol.V4.Frames.Requests.AuthResponse do

  alias OverDB.Protocol.V4.Frames.Frame

  @type t :: :auth_response

  def opcode() do
    0x0F
  end

  @spec new(map) :: list
  def new(options) do
    auth = Map.get(options, :auth, Application.get_env(:over_db, :auth))
    body = [0x00, Keyword.fetch!(auth, :username), 0x00, Keyword.fetch!(auth, :password)]
    Frame.create(:auth_response, body) |> encode() |> Frame.encode()
  end

  @spec encode(Frame.t) :: Frame.t
  def encode(%Frame{opcode: :auth_response, body: body} = frame) do
    %{frame | body: [<<IO.iodata_length(body)::32>>, body]}
  end

end
