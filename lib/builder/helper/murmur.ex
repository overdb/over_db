defmodule OverDB.Builder.Helper.Murmur do

  @spec create(binary) :: integer
  def create(pk_binary) do
    <<token::64-signed-little-integer, _::binary>> = :murmur.murmur3_cassandra_x64_128(pk_binary)
    token
  end

end
