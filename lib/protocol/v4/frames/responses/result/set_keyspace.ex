defmodule OverDB.Protocol.V4.Frames.Responses.Result.SetKeyspace do


  defstruct [:keyspace]

  @type t :: %__MODULE__{keyspace: String.t}

  @spec create(String.t) :: t
  def create(keyspace) do
    %__MODULE__{keyspace: keyspace}
  end


end
