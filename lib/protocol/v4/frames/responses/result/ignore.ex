defmodule OverDB.Protocol.V4.Frames.Responses.Result.Ignore do

  # NOTE: this response struct is not part of CQL v4 protocol.

  defstruct [:state, :reason]

  @type t :: %__MODULE__{state: map, reason: atom}

  @spec create(map, atom) :: t
  def create(state, reason) do
    %__MODULE__{state: state, reason: reason}
  end


end
