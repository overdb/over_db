defmodule OverDB.Protocol.V4.Frames.Header.Flags.Tracing do

  @type t :: :tracing

  def flag() do
    0x02
  end

end
