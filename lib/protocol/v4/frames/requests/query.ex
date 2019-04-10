defmodule OverDB.Protocol.V4.Frames.Requests.Query do
  @moduledoc """
  Documentation for Query.

  Performs a CQL query. The body of the message must be:
    <query><query_parameters>
  where <query> is a [long string] representing the query and
  <query_parameters> must be
    <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  where:
    - <consistency> is the [consistency] level for the operation.
    - <flags> is a [byte] whose bits define the options for this query and
      in particular influence what the remainder of the message contains.
      A flag is set if the bit corresponding to its `mask` is set. Supported
      flags are, given their mask:
        0x01: Values. If set, a [short] <n> followed by <n> [value]
              values are provided. Those values are used for bound variables in
              the query. Optionally, if the 0x40 flag is present, each value
              will be preceded by a [string] name, representing the name of
              the marker the value must be bound to.
        0x02: Skip_metadata. If set, the Result Set returned as a response
              to the query (if any) will have the NO_METADATA flag (see
              Section 4.2.5.2).
        0x04: Page_size. If set, <result_page_size> is an [int]
              controlling the desired page size of the result (in CQL3 rows).
              See the section on paging (Section 8) for more details.
        0x08: With_paging_state. If set, <paging_state> should be present.
              <paging_state> is a [bytes] value that should have been returned
              in a result set (Section 4.2.5.2). The query will be
              executed but starting from a given paging state. This is also to
              continue paging on a different node than the one where it
              started (See Section 8 for more details).
        0x10: With serial consistency. If set, <serial_consistency> should be
              present. <serial_consistency> is the [consistency] level for the
              serial phase of conditional updates. That consitency can only be
              either SERIAL or LOCAL_SERIAL and if not present, it defaults to
              SERIAL. This option will be ignored for anything else other than a
              conditional update/insert.
        0x20: With default timestamp. If set, <timestamp> should be present.
              <timestamp> is a [long] representing the default timestamp for the query
              in microseconds (negative values are forbidden). This will
              replace the server side assigned timestamp as default timestamp.
              Note that a timestamp in the query itself will still OverDBride
              this timestamp. This is entirely optional.
        0x40: With names for values. This only makes sense if the 0x01 flag is set and
              is ignored otherwise. If present, the values from the 0x01 flag will
              be preceded by a name (see above). Note that this is only useful for
              QUERY requests where named bind markers are used; for EXECUTE statements,
              since the names for the expected values was returned during preparation,
              a client can always provide values in the right order without any names
              and using this flag, while supported, is almost surely inefficient.

  Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
  TRUNCATE, ...).

  The server will respond to a QUERY message with a RESULT message, the content
  of which depends on the query.
  """

  alias OverDB.Protocol.V4.Frames.{Frame, Requests.Encoder}

  @type a :: :query
  @type t :: %__MODULE__{statement: String.t, values: list | map, metadata: list}
  defstruct [:statement, :values, :metadata]

  def opcode() do
    0x07
  end

  @spec create(binary, list) :: list
  def create(statement, values \\ [], metadata \\ []) do
    %__MODULE__{statement: statement, values: values, metadata: metadata}
  end

  @spec new(t, map) :: list
  def new(%__MODULE__{statement: statement, values: values, metadata: metadata}, opts \\ %{}) do
    flags = Map.get(opts, :flags, %{ignore: true})
    Frame.create(:query, Encoder.query_new_body(statement, values, metadata, opts), flags) |> Frame.encode()
  end

  @spec push_alone(t, map) :: list
  def push_alone(%__MODULE__{statement: statement, values: values, metadata: metadata}, opts \\ %{}) do
    flags = Map.get(opts, :flags, %{ignore: true})
    Frame.create(:query, Encoder.query_new_body(statement, values, metadata, opts), flags) |> Frame.push()
  end

  # this to encode the query at the client_end before pushing it to the batcher
  # NOTE:  the opt is unused now, but it will have use cases in future (like forcing_timestamp for individual query inside batch, or asking for high consitency level)
  @spec push(t, map) :: list
  def push(%__MODULE__{statement: _statement, values: _values, metadata: _metadata} = query, opts \\ %{}) do
    Encoder.query_in_batch(query, opts)
  end

end
