defmodule OverDB.Protocol.V4.Frames.Responses.Result.Compute do

  # NOTE: this response struct is not part of CQL v4 protocol.

  defstruct [:result, :function, :result_of]

  @type t :: %__MODULE__{result: term, function: tuple, result_of: atom}

  @spec create(term, tuple, atom) :: t
  def create(result, function, result_of) do
    %__MODULE__{result: result, function: function, result_of: result_of}
  end


end
