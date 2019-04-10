defmodule OverDB.Protocol.V4.Frames.Header.Flags.CustomPayload do

@type t :: :custom_payload

  def flag() do
    0x04
  end

end
