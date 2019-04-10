defmodule OverDB.Protocol.V4.Frames.Responses.Error do

  alias OverDB.Protocol.V4.Frames.Responses.Decoder
  alias OverDB.Protocol.V4.Frames.Frame

  defexception [:reason, :message]

  @type t :: %__MODULE__{reason: atom, message: String.t}

  @error_codes %{
    0x0000 => :server_failure,
    0x000A => :protocol_violation,
    0x0100 => :invalid_credentials,
    0x1000 => :unavailable,
    0x1001 => :overloaded,
    0x1002 => :bootstrapping,
    0x1003 => :truncate_failure,
    0x1100 => :write_timeout,
    0x1200 => :read_timeout,
    0x1300 => :read_failed,
    0x2000 => :invalid_syntax,
    0x2100 => :unauthorized,
    0x2200 => :invalid,
    0x2300 => :invalid_config,
    0x2400 => :already_exists,
    0x2500 => :unprepared,
  }

  for {code, reason} <- @error_codes do
    @spec decode(binary) :: {atom, binary}
    defp decode_reason(<<unquote(code)::32-signed, buffer::binary>>) do
      {unquote(reason), buffer}
    end
  end

  @spec decode_message(binary) :: String.t
  defp decode_message(buffer) do
    {message, _} = Decoder.string(buffer)
    message
  end

  # Error decoding
  @spec decode(Frame.t) :: t
  def decode(%Frame{opcode: :error, body: body}) do
    {reason, buffer} = decode_reason(body)
    create(reason, decode_message(buffer))
  end

  @spec create(atom, String.t) :: t
  def create(reason, message) when is_binary(message) and is_atom(reason) do
    %__MODULE__{reason: reason, message: message}
  end

  def opcode() do
    0x00
  end

end
