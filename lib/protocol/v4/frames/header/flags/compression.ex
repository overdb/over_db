defmodule OverDB.Protocol.V4.Frames.Header.Flags.Compression do

@type t :: :compression

  def flag() do
    0x01
  end

end
