defmodule OverDB.Protocol.V4.Frames.Header.Flags.Warning do

  @type t :: :warning

  def flag() do
    0x08
  end

end
