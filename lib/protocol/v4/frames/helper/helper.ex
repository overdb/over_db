defmodule OverDB.Protocol.V4.Frames.Helper do
  use Bitwise

  def varint_size(x), do: varint_size(x, 0)
  defp varint_size(x, acc) when x <= 127 and x >= 0,  do: acc + 1
  defp varint_size(x, acc) when x > 127 and x < 256,  do: acc + 2
  defp varint_size(x, acc) when x < 0 and x >= -128,  do: acc + 1
  defp varint_size(x, acc) when x < -128 and x >= -256,  do: acc + 2
  defp varint_size(x, acc) , do: varint_size(bsr(x, 8), acc + 1)

end
