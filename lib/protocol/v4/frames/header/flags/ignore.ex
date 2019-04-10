defmodule OverDB.Protocol.V4.Frames.Header.Flags.Ignore do

  @type t :: :ignore

  def flag() do
    0x00  # assuming that ingoring value whould be other than [0x01, 0x02, 0x04, 0x08 AND the masked flags]
  end

end
