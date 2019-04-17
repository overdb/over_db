defmodule OverDB.Protocol.V4.Frames.Requests.Encoder do

  @moduledoc false

  use Bitwise

  alias OverDB.Protocol.V4.Frames.{Helper, Requests.Query, Responses.Result.Prepared}

  @type data :: integer | binary | float | map | list | tuple | struct | boolean | atom

  @default_consistency :one
  @default_page_size 5_000
  @null << 255, 255, 255, 255 >>
  @ref_days 719528
  @intial_mask 0x00
  @unix_epoch_days 0x80000000
  @consistency_levels %{
    0x0000 => :any,
    0x0001 => :one,
    0x0002 => :two,
    0x0003 => :three,
    0x0004 => :quorum,
    0x0005 => :all,
    0x0006 => :local_quorum,
    0x0007 => :each_quorum,
    0x0008 => :serial,
    0x0009 => :local_serial,
    0x000A => :local_one,
  }

  defp serial_consistency?(nil), do: []

  for {code, level} <- @consistency_levels do
    defp consistency?(unquote(level)) do
      <<unquote(code)::16-unsigned-integer>>
    end
    if level in [:serial, :local_serial] do
      defp serial_consistency?(unquote(level)) do
        <<unquote(code)::16-unsigned-integer>>
      end
    else
      defp serial_consistency?(unquote(level)) do
        raise ArgumentError, ":serial_consistency option can't be #{unquote(level)}, only [:serial, :local_serial] are supported"
      end
    end
  end

  @flags %{
    0x02 => :skip_metadata?,
    0x04 => :page_size,
    0x08 => :paging_state,
    0x10 => :serial_consistency,
    0x20 => :timestamp
  }

  for {code, atom} <- @flags do
    defp atom_to_flag?(unquote(atom)) do
      unquote(code)
    end
  end

  defguardp is_prepared_list?(columns, list) when is_list(list) and length(columns) == length(list)
  defguardp is_prepared_map?(columns, map) when is_map(map) and length(columns) == map_size(map)

  # public funcs

  @spec compute_partition_key(list, map) :: list | binary
  def compute_partition_key(list, schema) do
    partition_key = schema.partition_key
    if length(partition_key) != 1 do
      composite_partition_key(list, schema)
    else
      pk = hd(partition_key)
      [data(schema.columns[pk][:type], list[pk])]
    end
  end


  # this for normal_queries so metadata_data should not be skiped by default,
  # but for stream_queries (stream queries only for read queries with non_compressed body_response) you should set it to true in the options,
  # and use the local schema inside the query to create columns_metadata to use it in decoding the stream_responses
  @spec query_new_body(binary, list | map, list, map) :: list
  def query_new_body(statement, values, metadata, options) when is_binary(statement) do
    skip_metadata = options[:skip_metadata?] || false
    [<<byte_size(statement)::32>>, statement, data(metadata, values, Map.put(options, :skip_metadata?, skip_metadata))]
  end

  # this for write_prepared_queries so metadata_data is not important
  @spec execute_new_body(binary, list, nil, list | map, map) :: list
  def execute_new_body(id, metadata, nil, values, options) do
    [<<byte_size(id)::16>>, id, data(metadata, values, Map.put(options, :skip_metadata?, true))] # changed from false to true.
  end

  # this for read_prepared_queries so metadata_data should be skiped
  @spec execute_new_body(binary, list, list, list | map, map) :: list
  def execute_new_body(id, metadata, _result_metadata, values, options) do
    [<<byte_size(id)::16>>, id, data(metadata, values, Map.put(options, :skip_metadata?, true))]
  end

  # this function must recv an already encoded queies_list, this is intended to boost the performance.
  @spec batch_push_body(list, atom, map) :: list
  def batch_push_body(queries, type, opts) do
    consistency = Map.get(opts, :consistency, :one)
    serial_consistency = Map.get(opts, :serial_consistency)
    timestamp = Map.get(opts, :timestamp)
    [
    batch_type(type),
    [<<length(queries)::16>>, queries],
    consistency?(consistency),
    @intial_mask
    |> set_flag(:serial_consistency, serial_consistency)
    |> set_flag(:timestamp, timestamp),
    serial_consistency?(serial_consistency),
    timestamp?(timestamp)
    ]
  end

  # this function must recv an a normal queies_list where each query is a struct(query).
  @spec batch_new_body(list, atom, map) :: list
  def batch_new_body(queries, type, opts) do
    consistency = Map.get(opts, :consistency, :one)
    serial_consistency = Map.get(opts, :serial_consistency)
    timestamp = Map.get(opts, :timestamp)
    [
    batch_type(type),
    for query <-  queries, into: [<<length(queries)::16>>] do
      query_in_batch(query, opts)
    end,
    consistency?(consistency),
    @intial_mask
    |> set_flag(:serial_consistency, serial_consistency)
    |> set_flag(:timestamp, timestamp),
    serial_consistency?(serial_consistency),
    timestamp?(timestamp)
    ]
  end

  # private funcs
  @spec data(list, list | map, map) :: list
  defp data(columns, data, options) do
    consistency = Map.get(options, :consistency, @default_consistency)
    skip_metadata? = Map.get(options, :skip_metadata?, false)
    page_size = Map.get(options, :page_size, @default_page_size)
    paging_state = Map.get(options, :paging_state)
    serial_consistency = Map.get(options, :serial_consistency)
    timestamp = Map.get(options, :timestamp)

    [
    consistency?(consistency),
    set_query_flag(data)
    |> set_flag(:skip_metadata?, skip_metadata?)
    |> set_flag(:page_size, true)
    |> set_flag(:paging_state, paging_state)
    |> set_flag(:serial_consistency, serial_consistency)
    |> set_flag(:timestamp, timestamp),
    if data == [] or data == %{} do
      []
    else
      values(columns, data, options)
    end,
    page_size?(page_size),
    paging_state?(paging_state),
    serial_consistency?(serial_consistency),
    timestamp?(timestamp)
    ]
  end

  # the :query functions are for unprepared query,
  # right now the the v should have the type as it's first tuple element,
  # TODO: but we are going to implement an automatic type generators for query type
  # which mean the user will only have to pass only order senstive values_list,
  # or just a simple named_key map without mentioning the type, or even better a keyword_list.

  @spec values(list, map, map) :: list
  defp values([], map, _opts) when is_map(map) do
    for {k, v} <- map, into: <<map_size(map)::16>> do
      k = to_string(k)
      <<byte_size(k)::16, k::binary, query_data(v)::binary>>
    end
  end

  # this function handle the query_parameters for both unprepared and prepared.
  # it takes encoded_values_structure [{type, value}, etc], you can use it directly through the protocol
  # or better use OverDB QueryBuilder and let it do the work for you
  @spec values(list, list, map) :: list
  defp values([], list, _opts) when is_list(list) do
    for value <- list, into: <<length(list)::16>> do
      query_data(value)
    end
  end



  # this function for prepared queries with named_key query like # NOTE: we dont recommend it
  # "SELECT * FROM user WHERE user_id = :user_id" note: not a user_id = ?
  @spec values(list, map, map) :: list
  defp values(columns, map, _opts) when is_prepared_map?(columns, map) do
    for {_k, _t, column, type} <- columns, into: [<<map_size(map)::16>>] do
      <<byte_size("#{column}")::16, "#{column}", query_data(type, map[column])>>
    end
  end

  # this function for prepared queries with named_key but like this
  # "SELECT * FROM user WHERE user_id = ?" note: not a user_id = :user_id.
  # you can still prepare the query with named_key and use it as normal but in our case we don't have to
  # as it will be inefficient, so instead we implement a better logic around it.
  # Now you can pass list_values in sensitive_order, or just pass a Keyword_list with named_keys,
  # or just pass the encoded values which OverDB QueryBuilder generated for you (# NOTE:  this is the default option and
  # to use other options you should pass values_structure: (:named for named_keyword, or :list for sensitive_order list_values).
  # Why we did implement this while we can have a named_key map ? this enable us to remove the
  # 0x40 flag from the query and get the same benfits like the named_key map. Please check CQL protocol V4 spec for
  # more details..
  @spec values(list, list, map) :: list
  defp values(columns, list, opts) when is_prepared_list?(columns, list) do
    case opts[:values_structure] do
      nil ->
        values([], list, opts)
      :named ->
        for {_k, _t, name, type} <- columns, into: <<length(list)::16>> do
          query_data(type, list[name])
        end
      :list ->
        for {{_k, _t, _name, type}, element} <- Enum.zip(columns, list), into: <<length(columns)::16>> do
          query_data(type, element)
        end
    end
  end

  @spec query_data({atom | tuple, binary}) :: list
  defp query_data({type, data}) when is_atom(type) or is_tuple(type) do
    query_data(type, data)
  end

  @spec query_data(atom | tuple, nil) :: binary
  defp query_data(_type, nil) do
    @null
  end


  @spec query_data(atom | tuple, binary) :: list
  defp query_data(type, [{sign, data}]) when sign in [:=, :in, :<=, :>=, :>, :<, :+, :-, :++] do
      query_data(type, data)
  end


  @spec query_data(atom | tuple, binary) :: list
  defp query_data(type, data) do
    io = data(type, data)
    <<IO.iodata_length(io)::32, io::binary>>
  end

  @spec data(atom, term) :: list | binary
  defp data(type, [{sign, data}]) when sign in [:=, :in, :<=, :>=, :>, :<, :+, :-, :++] do
    data(type, data)
  end

  @spec data(atom, boolean) :: list
  defp data(:boolean, true) do
    <<1>>
  end

  @spec data(atom, boolean) :: list
  defp data(:boolean, false) do
    <<0>>
  end

  @spec data(atom, binary) :: binary
  defp data(:ascii, data) when is_binary(data) do
    data
  end

  @spec data(atom, atom) :: binary
  defp data(:ascii, data) when is_atom(data) do
    :erlang.atom_to_binary(data, :latin1)
  end

  @spec data(atom, binary) :: binary
  defp data(:text, data) when is_binary(data) do
    data
  end

  @spec data(atom, binary) :: binary
  defp data(:varchar, data) when is_binary(data) do
    data
  end

  @spec data(atom, integer) :: binary
  defp data(:timestamp, data) when is_integer(data) do
    <<data::64>>
  end

  @spec data(atom, struct) :: binary
  defp data(:timestamp, %DateTime{} = data) do
    <<DateTime.to_unix(data, :milliseconds)::64>>
  end

  @spec data(atom, integer) :: binary
  defp data(:time, data) do
    <<data::64>>
  end


  @spec data(atom, binary) :: binary
  defp data(:uuid, <<_uuid::16-bytes>> = data) do
    data
  end

  @spec data(atom, binary) :: binary
  defp data(:timeuuid, <<_timeuuid::16-bytes>> = data) do
    data
  end


  @spec data(atom, binary) :: binary
  defp data(:uuid, <<_uuid::36-bytes>> = data) do
    layout_uuid(data)
  end


  @spec data(atom, binary) :: binary
  defp data(:timeuuid, <<_timeuuid::36-bytes>> = data) do
    layout_uuid(data)
  end


  @spec data(atom, integer) :: binary
  defp data(:bigint, data) when is_integer(data) do # TODO:  like what we did to :int type.
    <<data::64-big-integer>>
  end

  @spec data(atom, binary) :: binary
  defp data(:blob, data) when is_binary(data) do
    data
  end

  @spec data(atom, integer) :: binary
  defp data(:counter, data) when is_integer(data) do # TODO:  like what we did to :int type.
    <<data::64-signed-integer>>
  end

  @spec data(atom, tuple) :: binary
  defp data(:date, date) when is_tuple(date) do
    days = (:calendar.date_to_gregorian_days(date) - @ref_days + @unix_epoch_days)
    <<days::32-big-unsigned-integer>>
  end

  @spec data(atom, integer) :: binary
  defp data(:date, days) when is_integer(days) do # TODO: maybe like what we did to :int type.
    <<(days + @unix_epoch_days)::32-big-unsigned-integer>>
  end

  @spec data(atom, tuple) :: binary
  defp data(:date, date) when is_tuple(date) do
    days = (:calendar.date_to_gregorian_days(date) - @ref_days + @unix_epoch_days)
    <<days::32-big-unsigned-integer>>
  end


  @spec data(atom, tuple) :: list
  defp data(:decimal, {unscaled, scale}) do # TODO: likely will have to check the decimal size(only scale) (like what we did with int types)
    <<scale::32-big-unsigned-integer, data(:varint, unscaled)::binary>>
  end

  @spec data(atom, float) :: binary
  defp data(:double, data) do # TODO: likely will have to check the double-float size (like what we did with int types)
    <<data::64-float>>
  end


  @spec data(atom, float) :: binary
  defp data(:float, data) do # TODO: likely will have to check the float size (like what we did with int types)
    <<data::32-float>>
  end


  @spec data(atom, tuple) :: binary
  defp data(:inet, {a, b, c, d}) do
    <<a, b, c, d>>
  end

  @spec data(atom, tuple) :: binary
  defp data(:inet, {a, b, c, d, e, f, h, g}) do
    <<a::16, b::16, c::16, d::16, e::16, f::16, h::16, g::16>>
  end


  @spec data(atom, integer) :: binary
  defp data(:int, data) when is_integer(data) do
    size = Helper.varint_size(data)
    if size <= 4 do
      <<data::32>>
    else
      # this enable us to get an error response for integer larger than 32bits,
      # otherwise it will encode a large int as 32int tho inserting a wrong int value into scylla. (because elixir doesn't have integer types.)
      <<data::size(size)-unit(8)>>
    end
  end

  @spec data(atom, integer) :: binary
  defp data(:smallint, data) when is_integer(data) do
    size = Helper.varint_size(data)
    if size <= 2 do
      <<data::16>>
    else
      # this enable us to get an error response for integer larger than 16bits,
      # otherwise it will encode a large int as 16int tho inserting a wrong int value into scylla. (because elixir doesn't have integer types.)
      <<data::size(size)-unit(8)>>
    end
  end

  @spec data(atom, integer) :: binary
  defp data(:tinyint, data) when is_integer(data) do # data is going to be encoded as 8bit by default. so nothing is needed.
    <<data>>
  end

  @spec data(atom, integer) :: binary
  defp data(:varint, data) when is_integer(data) do
    size = Helper.varint_size(data)
    <<data::size(size)-unit(8)>>
  end

  @spec data(atom, integer) :: binary
  defp data(:varint, data) when is_binary(data) do
    data
  end

  @spec data(atom, list) :: list
  defp data({:list, e_type}, data) when is_list(data) do
    for element <- data, into: <<length(data)::32>> , do: query_data(e_type, element)
  end

  @spec data(map, map) :: list
  defp data({:map, k_type, v_type}, data) when is_map(data) do
    for {k, v} <- data, into: <<map_size(data)::32>>, do: << query_data(k_type, k), query_data(v_type, v) >>
  end

  @spec data(tuple, list) :: list
  defp data({:set, in_type}, %MapSet{} = data) do
    data({:list, in_type}, MapSet.to_list(data))
  end

  @spec data(tuple, list) :: list
  defp data({:tuple, types}, data) when is_list(data) do
    for {type, element} <- Enum.zip(types, data) do
      query_data(type, element)
    end
  end

  @spec data(tuple, list) :: list
  defp data({:tuple, types}, data) when is_tuple(data) do
    for {type, element} <- Enum.zip(types, Tuple.to_list(data)) do
      query_data(type, element)
    end
  end

  @spec data(tuple, list) :: list
  defp data({:udt, fields}, data) when is_map(data) do
    for {name, type} <- fields do
      query_data(type, Map.get(data, name))
    end
  end


  @spec bytes(binary) :: binary
  defp bytes(bytes) when byte_size(bytes) >= 0 do
    <<byte_size(bytes)::32, bytes::binary>>
  end

  # this is needed for custom_payload feature
  @spec bytes_map(map) :: list
  def bytes_map(map) do
    for {k, v} <- map, into: <<map_size(map)::16>> do
      k = to_binary(k)
      v = to_binary(v)
      <<byte_size(k)::16, k::binary, bytes(v)::binary>>
    end
  end

  @spec string_map(map) :: list
  def string_map(map) do
    for {k, v} <- map, into: <<map_size(map)::16>> do
      k = to_binary(k)
      v = to_binary(v)
      <<byte_size(k)::16, k::binary, byte_size(v)::16, v::binary>>
    end
  end

  @spec string_list(list) :: list
  def string_list(list) do
    for element <- list, into: <<length(list)::16>> do
      <<byte_size(element)::16, element::binary>>
    end
  end

  @spec batch_type(atom) :: integer
  defp batch_type(:logged), do: 0
  defp batch_type(:unlogged), do: 1
  defp batch_type(:counter), do: 2

  @spec query_in_batch(map, map) :: list
  def query_in_batch(%Query{statement: statement, values: values, metadata: metadata}, opts) do
    [
      0, # this indicates a normal_unprepared_query
      <<byte_size(statement)::32>>,
      statement, values(metadata, values, opts)
    ]
  end

  @spec query_in_batch({atom, list, list, list}, map) :: list
  def query_in_batch(%Prepared{id: id, metadata: metadata, values: values}, opts) do
    [
      1, # this indicates a prepared query
      <<byte_size(id)::16>>,
      id, values(metadata, values, opts)
    ]
  end


  @spec to_binary(atom) :: binary
  defp to_binary(atom) when is_atom(atom) do
    :erlang.atom_to_binary(atom, :latin1)
  end

  @spec to_binary(list) :: binary
  defp to_binary(list) when is_list(list) do
    :erlang.list_to_binary(list)
  end

  @spec to_binary(binary) :: binary
  defp to_binary(binary) when is_binary(binary) do
    binary
  end

  @spec timestamp?(nil) :: []
  defp timestamp?(nil), do: []

  @spec timestamp?(integer) :: binary
  defp timestamp?(timestamp), do: <<timestamp::64>>

  @spec page_size?(integer) :: binary
  defp page_size?(size), do: <<size::32>>

  @spec paging_state?(nil) :: []
  defp paging_state?(nil), do: []

  @spec paging_state?(binary) :: list
  defp paging_state?(v) when is_binary(v), do: [<<byte_size(v)::32>>, v]

  @spec set_query_flag([]) :: integer
  defp set_query_flag([]), do: 0

  @spec set_query_flag(%{}) :: integer
  defp set_query_flag(map) when map == %{}, do: 0

  @spec set_query_flag(list) :: integer
  defp set_query_flag(list) when is_list(list), do: 1

  @spec set_query_flag(map) :: integer
  defp set_query_flag(map) when is_map(map), do: 65

  @spec set_flag(integer, atom, boolean) :: integer
  defp set_flag(integer, atom, boolean) when is_atom(atom), do: set_flag(integer, atom_to_flag?(atom), boolean)

  @spec set_flag(integer, integer, boolean) :: integer
  defp set_flag(mask, _int, nil), do: mask

  @spec set_flag(integer, integer, boolean) :: integer
  defp set_flag(mask, _int, false), do: mask

  @spec set_flag(integer, integer, boolean) :: integer
  defp set_flag(mask, int, true), do:  mask ||| int

  # check https://en.wikipedia.org/wiki/Universally_unique_identifier for more details
  @spec layout_uuid(binary) :: binary
  defp layout_uuid(<<a::8-bytes, _, b::4-bytes, _, c::4-bytes, _, d::4-bytes, _, e::12-bytes>>) do
    <<Base.decode16!(a)::4-bytes, Base.decode16!(b)::2-bytes, Base.decode16!(c)::2-bytes, Base.decode16!(d)::2-bytes, Base.decode16!(e)::6-bytes>>
  end

  @spec component_partition_key(atom | tuple, term) :: list
  defp component_partition_key(type, value) do
    io = data(type, value)
    [<<IO.iodata_length(io)::16>>, io, [0]]
  end

  @spec composite_partition_key(list, map) :: list
  defp composite_partition_key(list, schema) do
    columns = schema.columns
    for pk <- schema.partition_key do
      component_partition_key(columns[pk][:type], list[pk])
    end
  end

end
