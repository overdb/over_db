defmodule OverDB.Ring.Shard do
  use Bitwise

  def lookup(token, nr_shards, ignore_msg) do
    biased_token = token +  (1 <<< 63)
    biased_token = biased_token <<< ignore_msg
    tokl = biased_token &&& 0xffffffff
    tokh = (biased_token >>> 32) &&& 0xffffffff
    mul1 = tokl * nr_shards
    mul2 = tokh * nr_shards
    sum = (mul1 >>> 32) + mul2
    sum >>> 32
  end

  def lookup_shard(ring, token, rf) do
    {ring_key, nr, ig} =
      ring.lookup_replicas_atom(token) |> Enum.take(rf) |> Enum.random()
    {ring_key, lookup(token, nr, ig)}
  end

  def lookup_batcher({ring_key, shard},conn, b_type, b_num) do
    FastGlobal.get(ring_key)[shard][conn][b_type][b_num]
  end

  def lookup_reporter({ring_key, shard},conn, stream_id \\ :rand.uniform(32768)) do
    FastGlobal.get(ring_key)[shard][conn][stream_id]
  end

end
