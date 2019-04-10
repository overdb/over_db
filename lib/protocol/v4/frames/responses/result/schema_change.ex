defmodule OverDB.Protocol.V4.Frames.Responses.Result.SchemaChange do


  defstruct [:effect, :target, :options]

  @type t :: %__MODULE__{effect: String.t, target: String.t, options: String.t | tuple}

  @spec create(String.t, String.t, String.t | tuple) :: t
  def create(effect, target, opts) do
    %__MODULE__{effect: effect, target: target, options: opts}
  end


end
