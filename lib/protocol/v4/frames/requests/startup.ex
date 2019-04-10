defmodule OverDB.Protocol.V4.Frames.Requests.Startup do
  @moduledoc """
  STARTUP

  Initialize the connection. The server will respond by either a READY message
  (in which case the connection is ready for queries) or an AUTHENTICATE message
  (in which case credentials will need to be provided using AUTH_RESPONSE).

  This must be the first message of the connection, except for OPTIONS that can
  be sent before to find out the options supported by the server. Once the
  connection has been initialized, a client should not send any more STARTUP
  messages.

  The body is a [string map] of options. Possible options are:
    - "CQL_VERSION": the version of CQL to use. This option is mandatory and
      currently the only version supported is "3.0.0". Note that this is
      different from the protocol version.
    - "COMPRESSION": the compression algorithm to use for frames (See section 5).
      This is optional; if not specified no compression will be used.
    - "NO_COMPACT": whether or not connection has to be established in compatibility
      mode. This mode will make all Thrift and Compact Tables to be exposed as if
      they were CQL Tables. This is optional; if not specified, the option will
      not be used.

  """

  alias OverDB.Protocol.V4.Frames.{Requests.Encoder, Frame}

  @type t :: :startup

  def opcode() do
    0x01
  end

  def new(opts) when is_map(opts) do
    Frame.create(:startup, Encoder.string_map(opts)) |> Frame.encode()
  end

end
