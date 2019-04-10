defmodule OverDB.Protocol.V4.Frames.Responses.Result.Rows do

  alias OverDB.Protocol.V4.Frames.Responses.Result.Compute

  defstruct [:page, :paging_state, :has_more_pages?, :columns]

  @type t :: %__MODULE__{page: list | Compute.t, paging_state: binary, has_more_pages?: boolean, columns: list}

  @spec create(list | Compute.t, binary, boolean, list) :: t
  def create(rows, paging_state, has_more_pages, columns) do
    %__MODULE__{page: rows, paging_state: paging_state, has_more_pages?: has_more_pages, columns: columns}
  end


end
