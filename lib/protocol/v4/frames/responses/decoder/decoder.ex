defmodule OverDB.Protocol.V4.Frames.Responses.Decoder do

  use Bitwise
  alias OverDB.Protocol.V4.Frames.Responses.Result.{Prepared, Rows, SetKeyspace, SchemaChange, Void, Compute}
  @unix_epoch_days 0x80000000

  # Types Decoding functions start

  @custom_types %{
    "org.apache.cassandra.db.marshal.ShortType" => :smallint,
    "org.apache.cassandra.db.marshal.SimpleDateType" => :date,
    "org.apache.cassandra.db.marshal.ByteType" => :tinyint,
    "org.apache.cassandra.db.marshal.TimeType" => :time,
  }

  for {type_class, type} <- @custom_types do
    defp custom_type_to_atom(unquote(type_class)) do
      unquote(type)
    end
  end

  defp custom_type_to_atom(type_class) do
    raise "invalid custom type, #{inspect(type_class)}, please let us know to support the new custom type"
  end

  @types %{
    1 => :ascii,
    2 => :bigint,
    3 => :blob,
    4 => :boolean,
    5 => :counter,
    6 => :decimal,
    7 => :double,
    8 => :float,
    9 => :int,
    11 => :timestamp,
    12 => :uuid,
    13 => :varchar,
    14 => :varint,
    15 => :timeuuid,
    16 => :inet,
    17 => :date,
    18 => :time,
    19 => :smallint,
    20 => :tinyint,
  }
  for {type_int, type} <- @types do
    @spec type(binary) :: {atom, atom, binary}
    defp type(<<unquote(type_int)::16, buffer::binary>>) do
      {:ok, unquote(type), buffer}
    end
  end

  @spec type(binary) :: {atom, tuple, binary}
  defp type(<<0x0000::16, buffer::binary>>) do
    {:ok, custom_type, buffer} = string(buffer)
    {:ok, custom_type_to_atom(custom_type), buffer}
  end

  @spec type(binary) :: {atom, tuple, binary}
  defp type(<<0x0020::16, buffer::binary>>) do
    {:ok, type, buffer} = type(buffer)
    {:ok, {:list, type}, buffer}
  end

  @spec type(binary) :: {atom, tuple, binary}
  defp type(<<0x0021::16, buffer::binary>>) do
    {:ok, key_type, buffer} = type(buffer)
    {:ok, value_type, buffer} = type(buffer)
    {:ok, {:map, key_type, value_type}, buffer}
  end

  @spec type(binary) :: {atom, tuple, binary}
  defp type(<<0x0031::16, length::16, buffer::binary>>) do
    {:ok, types, buffer} = tuple_types(length, buffer)
    {:ok, {:tuple, types}, buffer}
  end

  @spec type(binary) :: {atom, tuple, binary}
  defp type(<<0x0022::16, buffer::binary>>) do
    {:ok, type, buffer} = type(buffer)
    {:ok, {:set, type}, buffer}
  end

  @spec type(binary) :: {atom, tuple, binary}
  defp type(<<0x0030::16, buffer::binary>>) do
    {:ok, _keyspace_name, buffer} = string(buffer)
    {:ok, _type_name, <<length::16, rest::binary>>} = string(buffer)
    {:ok, types, buffer} = udt_types(length, rest)
    {:ok, {:udt, types}, buffer}
  end

  @spec type(binary) :: {atom, atom, binary}
  defp type(<<_::16, buffer::binary>>) do
    {:ok, :unknown, buffer}
  end

  @spec tuple_types(integer, binary) :: {atom, list, binary}
  defp tuple_types(length, buffer) do
    {:ok, types, buffer} = tuple_types(length, [], buffer)
    {:ok, :lists.reverse(types), buffer}
  end

  @spec tuple_types(0, list, binary) :: {atom, list, binary}
  defp tuple_types(0, acc, buffer) do
    {:ok, acc, buffer}
  end

  @spec tuple_types(integer, list, binary) :: {atom, list, binary}
  defp tuple_types(length, acc, buffer) do
    {:ok, type, rest1} = type(buffer)
    tuple_types(length - 1, [type | acc], rest1)
  end

  @spec udt_types(integer, binary) :: {atom, list, binary}
  defp udt_types(length, buffer) do
    {:ok, types, rest0} = udt_types(length, [], buffer)
    {:ok, :lists.reverse(types), rest0}
  end

  @spec udt_types(0, list, binary) :: {atom, list, binary}
  defp udt_types(0, acc, buffer) do
    {:ok, acc, buffer}
  end

  @spec udt_types(integer, list, binary) :: {atom, list, binary}
  defp udt_types(length, acc, buffer) do
    {:ok, col_name, rest1} = string(buffer)
    {:ok, col_type, rest2} = type(rest1)
    udt_types(length - 1, [{col_name, col_type}| acc], rest2)
  end


  # decoding the body of :ready response

  @spec ready(binary) :: :ready
  def ready(<<>>) do
    :ready
  end

  # decoding the body for :void results

  @spec result(binary, map) :: Void.t
  def result(<<0x0001::32-signed>>, _opts) do
    Void.create()
  end


  # decoding the body of the result of :rows response

  @spec result(binary, map) :: Rows.t
  def result(<<0x0002::32-signed, buffer::binary>>, opts) do
    {columns,columns_length, paging_state, has_more_pages, buffer} = result_metadata(buffer, :rows)
    {rows, <<>>} =  rows(buffer, columns, columns_length, opts)
    Rows.create(rows, paging_state, has_more_pages, columns)
  end

  # decoding the body of the result of :prepare query

  @spec result(binary, map) :: Prepared.t
  def result(<<0x0004::32-signed, buffer::binary>>, _opts) do
    {id, buffer} = string(buffer)
    {{metadata, metadata_length, pk_indices, nil}, buffer} = metadata(buffer)
    {{result_metadata, result_metadata_length,  nil}, <<>>} = result_metadata(buffer)
    Prepared.create(id, metadata, metadata_length, result_metadata, result_metadata_length, pk_indices)
  end


  # decoding the body for :set_keyspace results

  @spec result(binary, map) :: SetKeyspace.t
  def result(<<0x0003::32-signed, buffer::bits>>, _opts) do
    {keyspace, <<>>} = string(buffer)
    SetKeyspace.create(keyspace)
  end

  # decoding the body for :schema_change results

  @spec result(binary, map) :: SchemaChange.t
  def result(<<0x0005::32-signed, buffer::bits>>, _opts) do
    {effect, buffer} = string(buffer)
    {target, buffer} = string(buffer)
    options =
      case target do
        "KEYSPACE" ->
          {keyspace ,_buffer} = string(buffer)
          keyspace
        target when target in ["TABLE", "TYPE"] ->
          {keyspace, buffer} = string(buffer)
          {object, <<>>} = string(buffer)
          {keyspace, object}
      end
    SchemaChange.create(effect, target, options)
  end

  # decoding the body of the result of :prepared_select query with stream = false.

  @spec result(binary, Prepared.t, map) :: Rows.t
  def result(<<0x0002::32-signed, buffer::binary>>, %Prepared{} = prepared, opts) do
    {columns,columns_length, paging_state, has_more_pages, buffer} = result_metadata(buffer, prepared)
    {rows, <<>>} =  rows(buffer, columns, columns_length, opts)
    Rows.create(rows, paging_state, has_more_pages, columns)
  end

  # we ignore the prepared struct for all non select/stream responses
  # thats why we route it to normal result functions.
  @spec result(binary, map, map) :: Void.t | SchemaChange.t | SetKeyspace.t
  def result(buffer, _prepared, opts) do
    result(buffer, opts)
  end

  # decoding the stream of the result of :stream query
  # this function should be called on the body_response of the execute_request/query_request(with skip_metadata true)
  @spec streamed_result(binary) :: tuple
  def streamed_result(<<0x0002::32-signed, flags::32, columns_length::32, buffer::binary>>) do
    {_, has_more_pages?, true} = flags(flags)
    {paging_state, buffer} = check_paging_state(buffer, has_more_pages?)
    {:ok, columns_length, has_more_pages?, paging_state, buffer}
    #  {:ok, columns_length, false, false, buffer} # now we can use this buffer to decode row/cell streams
    #  {:ok, columns_length, true, <<>>, buffer} # this indicates that there is no enough bytes to decode the paging_state, the rest buffer should be added to the next stream and calling this function again till it gets the paging_state
    #  {:ok, columns_length, true, nil, buffer}  # now we can use the rest_buffer to decode row/cell streams # this is equal to false (# TODO: not confirmed yet)
    #  {:ok, columns_length, true, paging_state, buffer} # now we can use the rest_buffer to decode row/cell streams, and add paging_state for future requests
  end

  @spec streamed_result(binary) :: tuple
  def streamed_result(buffer) do
    {:short, buffer} # this indicates the body buffer is not enough at all.
  end

  @spec check_paging_state(binary, boolean) :: tuple
  defp check_paging_state(buffer, false) do
    {false, buffer}
  end

  @spec check_paging_state(binary, boolean) :: tuple
  defp check_paging_state(<<-1::32-signed, buffer::binary>>, true) do
    {nil, buffer}
  end

  @spec check_paging_state(binary, boolean) :: tuple
  defp check_paging_state(<<size::32, value::size(size)-bytes, buffer::binary>>, true) do
    {value, buffer}
  end
  @spec check_paging_state(binary, boolean) :: tuple
  defp check_paging_state(buffer, true) do
    {<<>>, buffer}
  end

  # we could have merged streamed_rows and rows functions in unified functions, but
  # this mean decoding a full response will be a little expensive with extra jump cycle inside the cpu_pipe for each row
  # so instead we created extra three functions to handle it. (for us every jump cycle and conditions order is matter for performance :)
  def streamed_rows(row, columns_count, columns, buffer, acc \\ [], opts \\ %{}, function \\ nil)
  @spec streamed_rows(0, integer, list, binary, term, map, tuple | nil) :: {term, binary}
  def streamed_rows(0, _columns_count, _columns, buffer, acc, _opts, function) do
    {reverse_or_compute(acc, function, :stream),0, buffer} # this indicates the end of a stream or an empty result.
  end

  # this is the public function for streamed_rows, you can pass the previous streamed_rows_list as acc (NOTE: this means calling :lists.reverse again and again :( ,
  # so it's better to keep it [] and handle/stream whatever row/s you get to the endpoint api (it's all depends on your application)
  @spec streamed_rows(integer, integer, list, binary, term, map, tuple | nil) :: {term, binary}
  def streamed_rows(row_count, columns_count, columns, buffer, acc, opts, function) do

    case row(columns_count, columns, buffer, [], opts) do
      {row, buffer} -> streamed_rows(row_count-1, columns_count, columns, buffer, row_compute(function, row, acc), opts, function)
      false -> {reverse_or_compute(acc, function, :stream),row_count, buffer} # this indicates that the buffer is not enough, so this function must be called again with this buffer <> next_stream_buffer, and row_count.
    end
  end

  @spec rows(binary, list, integer, map) :: {list, binary}
  defp rows(<<row_count::32-signed, buffer::binary>>, columns, columns_count, opts) do
    rows(row_count, columns_count, columns, buffer, [], opts, opts[:function])
  end

  @spec rows(0, integer, list, binary, list, map, tuple | nil) :: {list, binary}
  defp rows(0, _columns_count, _columns, buffer, acc, _opts, function) do
    {reverse_or_compute(acc, function, :select), buffer}
  end

  @spec rows(integer, integer, list, binary, list, map, tuple | nil) :: {list, binary}
  defp rows(row_count, columns_count, columns, buffer, acc, opts, function) do
    {row, buffer} = row(columns_count, columns, buffer, [], opts)
    rows(row_count-1, columns_count, columns, buffer, row_compute(function, row, acc), opts, function)
  end

  @spec row(0, list, binary, list, map) :: {list, binary}
  defp row(0, _, buffer, acc, _opts) do
    {:lists.reverse(acc), buffer}
  end

  @spec row(integer, list, binary, list, map) :: {list, binary}
  defp row(columns_count, [{_k, _t, n, _type} | columns], <<null::32-signed, buffer::binary>>, acc, opts) when null < 0  do
    row(columns_count-1, columns, buffer, [{n, nil} | acc], opts)
  end

  @spec row(integer, list, binary, list, map) :: {list, binary}
  defp row(columns_count, [{_k, _t, n, type} | columns], <<size::32, cell::size(size)-bytes, buffer::binary>>, acc, opts) do
    row(columns_count-1, columns, buffer, [ {n, data(type, cell, opts)} | acc], opts)
  end

  # this extra function to handle streamed row with _non_complete_buffer
  @spec row(integer, list, binary, list, map) :: boolean
  defp row(_columns_count, _, _non_complete_buffer, _acc, _opts) do
    false
  end

  @spec pk_indices(binary, integer) :: list
  defp pk_indices(buffer, pk_count) do
    pk_indices(buffer, pk_count, [])
  end

  @spec pk_indices(binary, 0, list) :: list
  defp pk_indices(buffer, 0, acc) do
    {:lists.reverse(acc), buffer}
  end

  @spec pk_indices(binary, integer, list) :: list
  defp pk_indices(<<pk_index::16-signed, rest::binary>>, pk_count, acc) do
    pk_indices(rest, pk_count-1, [pk_index | acc])
  end

  @spec metadata(binary) :: {{list, integer, list, binary | nil}, binary}
  defp metadata(<<flags::32, columns_length::32, pk_count::32, buffer::binary>>) do
    {pk_indices, buffer} = pk_indices(buffer, pk_count)
    {global_table_spec, has_more_pages, no_metadata} = flags(flags)
    {paging_state, buffer} = paging_state(buffer, has_more_pages)
    {metadata, buffer} = case no_metadata do
      true -> {[], buffer}
      false -> columns_metadata(buffer, columns_length, global_table_spec)
    end
    {{metadata, columns_length, pk_indices, paging_state},buffer}
  end

  @spec result_metadata(binary) :: {{list, integer, binary | nil}, binary}
  defp result_metadata(<<flags::32, columns_length::32, buffer::binary>>) do
    {global_table_spec, has_more_pages, no_metadata} = flags(flags)
    {paging_state, buffer} = paging_state(buffer, has_more_pages)
    {result_metadata, buffer} = case no_metadata do
      true -> {nil, buffer}
      false -> columns_metadata(buffer, columns_length, global_table_spec)
    end
    {{result_metadata, columns_length, paging_state} ,buffer}
  end

  @spec result_metadata(binary, Prepared.prepared | map) :: {{list, integer, binary | nil}, binary}
  defp result_metadata(<<flags::32, columns_length::32, buffer::binary>>, %{result_metadata: result_metadata}) do
    {_global_table_spec, has_more_pages, true} = flags(flags)
    {paging_state, buffer} = paging_state(buffer, has_more_pages)
    {result_metadata, columns_length, paging_state, has_more_pages, buffer}
  end


  @spec result_metadata(binary, atom) :: {{list, integer, binary | nil}, binary}
  defp result_metadata(<<flags::32, columns_length::32, buffer::binary>>, :rows) do
    {global_table_spec, has_more_pages, false} = flags(flags)
    {paging_state, buffer} = paging_state(buffer, has_more_pages)
    {result_metadata, buffer} = columns_metadata(buffer, columns_length, global_table_spec)
    {result_metadata, columns_length, paging_state, has_more_pages, buffer}
  end



  @spec flags(integer) :: {boolean, boolean, boolean}
  defp flags(flags) do
      global_table_spec = band(flags, 1) == 1
      has_more_pages = band(flags, 2) == 2
      no_metadata = band(flags, 4) == 4
      {global_table_spec, has_more_pages, no_metadata}
  end

  @spec paging_state(binary, true) :: {binary | nil, binary}
  defp paging_state(buffer, true) do
    bytes(buffer)
  end

  @spec paging_state(binary, false) :: {nil, binary}
  defp paging_state(buffer, false) do
    {nil, buffer}
  end

  @spec columns_metadata(binary, integer, true) :: {list, binary}
  defp columns_metadata(buffer, columns_count, true) do
    {keyspace, buffer} = string(buffer)
    {table, buffer} = string(buffer)
    columns_metadata(buffer, columns_count, {keyspace, table}, [])
  end

  @spec columns_metadata(binary, integer, false) :: {list, binary}
  defp columns_metadata(buffer, columns_count, false) do
    columns_metadata(buffer, columns_count, nil, [])
  end

  @spec columns_metadata(binary, integer, tuple | nil, list) :: {list, binary}
  defp columns_metadata(buffer, 0, _global_table_spec, acc) do
    {:lists.reverse(acc), buffer}
  end

  @spec columns_metadata(binary, integer, nil, list) :: {list, binary}
  defp columns_metadata(buffer, length ,nil = global_table_spec, acc) do
    {keyspace, buffer} = string(buffer)
    {table, buffer} = string(buffer)
    {name, buffer} = string(buffer)
    {:ok, type, buffer} = type(buffer)
    columns_metadata(buffer, length - 1, global_table_spec, [{keyspace, table, :"#{name}", type} | acc])
  end

  @spec columns_metadata(binary, integer, {String.t, String.t}, list) :: {list, binary}
  defp columns_metadata(buffer, length ,{keyspace, table} = global_table_spec, acc) do
    {name, buffer} = string(buffer)
    {:ok, type, buffer} = type(buffer)
    columns_metadata(buffer, length - 1, global_table_spec, [{keyspace, table, :"#{name}", type} | acc])
  end


  # data Decoding functions start

  @spec data(term, binary, map) :: nil
  defp data(_type, <<>>, _opts) do
    nil
  end

  @spec data(term, binary, map) :: nil
  defp data(:varchar, <<text::binary>>, _opts) do
    text
  end

  @spec data(atom, binary, map) :: boolean
  defp data(:boolean, <<boolean::8>>, _opts) do
    boolean != 0
  end

  @spec data(atom, binary, map) :: binary
  defp data(:blob, <<blob::binary>>, _opts) do
    blob
  end

  @spec data(atom, binary, map) :: integer
  defp data(:int, <<int::32-signed>>, _opt) do
    int
  end

  @spec data(atom, binary, map) :: integer
  defp data(:bigint, <<bigint::64-signed>>, _opt) do
    bigint
  end

  @spec data(atom, binary, map) :: integer
  defp data(:smallint, <<smallint::16-signed>>, _opt) do
    smallint
  end

  @spec data(atom, binary, map) :: integer
  defp data(:tinyint, <<tinyint::8-signed>>, _opt) do
    tinyint
  end

  @spec data(atom, binary, map) :: integer
  defp data(:double, <<double::64-signed>>, _opt) do
    double
  end

  @spec data(atom, binary, map) :: float
  defp data(:float, <<float::32-float>>, _opt) do
    float
  end

  @spec data(atom, binary, map) :: DateTime.t
  defp data(:timestamp, <<timestamp::64-signed>>, %{timestamp: :datetime}) do
    DateTime.from_unix!(timestamp, :milliseconds)
  end

  @spec data(atom, binary, map) :: integer
  defp data(:timestamp, <<timestamp::64-signed>>, _opts) do
    timestamp
  end

  @spec data(atom, binary, map) :: binary
  defp data(:uuid, <<a::4-bytes, b::2-bytes, c::2-bytes, d::2-bytes, e::6-bytes>>, _opts) do
    Base.encode16(a) <> "-" <> Base.encode16(b) <> "-" <> Base.encode16(c) <> "-" <>
    Base.encode16(d) <> "-" <> Base.encode16(e)
  end

  @spec data(atom, binary, map) :: binary
  defp data(:timeuuid, <<timeuuid::16-bytes>>, opts) do
    data(:uuid, timeuuid, opts)
  end

  @spec data(atom, binary, map) :: integer
  defp data(:counter, <<counter::64-signed>>, _opts) do
    counter
  end

  @spec data(atom, binary, map) :: binary
  defp data(<<ascii::bits>>, :ascii, _opts) do
    ascii
  end

  @spec data(atom, binary, map) :: integer
  defp data(:date, <<value::32-big-unsigned-integer>>, %{date: :days}) do
    (value - @unix_epoch_days)
  end

  @spec data(atom, binary, map) :: {integer, integer, integer}
  defp data(:date, <<value::32-signed>>, _opts) do
    (value - @unix_epoch_days) |> :calendar.gregorian_days_to_date()
  end

  @spec data(atom, binary, map) :: integer
  defp data(:varint, <<buffer::binary>>, _opt) do
    size = byte_size(buffer)
    <<varint::size(size)-big-signed-integer-unit(8)>> = buffer
    varint
  end

  # TODO: Support decimal library
  @spec data(atom, binary, map) :: tuple
  defp data(:decimal, <<scale::32-signed, data::binary>>, opts) do
    {_unscaled = data(:varint, data, opts), scale}
  end

  @spec data(atom, binary, map) :: tuple
  defp data(:inet, <<a::8, b::8, c::8, d::8>>, _opt) do
    {a, b, c, d}
  end

  @spec data(atom, binary, map) :: tuple
  defp data(:inet, <<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>, _opt) do
    {a, b, c, d, e, f, g, h}
  end

  @spec data(atom, binary, map) :: list
  defp data({:list, type}, <<_count::32-signed, data::binary>>, opts) do
    for <<size::32, value::size(size)-binary <- data>> do
      data(type, value, opts)
    end
  end

  @spec data(atom, binary, map) :: map
  defp data({:map, key_type, value_type}, <<_count::32-signed, data::binary>>, %{map: :atom} = opts) do
   for <<ksize::32, key::size(ksize)-binary, vsize::32, value::size(vsize)-binary <- data>> do
     {:"#{data(key_type, key, opts)}", data(value_type, value, opts)}
   end
   |> Enum.into(%{})
  end

  @spec data(atom, binary, map) :: map
  defp data({:map, key_type, value_type}, <<_count::32-signed, data::binary>>, opts) do
   for <<ksize::32, key::size(ksize)-binary, vsize::32, value::size(vsize)-binary <- data>> do
     {data(key_type, key, opts), data(value_type, value, opts)}
   end
   |> Enum.into(%{})
  end

  @spec data(atom, binary, map) :: Mapset.t
  defp data({:set, type}, <<_count::32-signed, data::binary>>, opts) do
    response =
      for <<size::32, value::size(size)-binary <- data>> do
        data(type, value, opts)
      end
    if opts[:set] == :list do
      response
    else
      response |> MapSet.new()
    end
  end


  @spec data(atom, binary, map) :: tuple
  defp data({:tuple, type}, <<data::binary>>, opts) do
    for <<size::32, value::size(size)-binary <- data>> do
      data(type, value, opts)
    end
    |> List.to_tuple()
  end

  @spec data(atom, binary, map) :: list | map
  defp data({:udt, fields}, <<data::binary>>, opts) do
    value_binary_list =
      for <<size::32-signed, value::size(size)-binary  <- data>> do
        if size < 0 do
          <<-1::32-signed>>
        else
          value
        end
      end
    list =
      for {{field_name, type}, value_binary} <- Enum.zip(fields, value_binary_list) do
        {:"#{field_name}", data(type, value_binary, opts)}
      end
    case Map.get(opts, :udt) do
      :map -> Enum.into(list, %{})
      _ -> list
    end
  end

  @spec string(binary) :: {String.t, binary}
  def string(<<length::16, string::size(length)-binary, rest::binary>> = buffer) when :erlang.size(buffer) >= length do
    {string, rest}
  end

  @spec string(binary) :: {atom, binary}
  def string(<<length::16, buffer::binary>> = buffer) when :erlang.size(buffer) < length do
    {:malformed_buffer, buffer}
  end

  @spec string_list(binary) :: {list, binary}
  def string_list(<<length::16, buffer::binary>>) do
    string_list(buffer, length, [])
  end

  @spec string_list(binary, 0, list) :: {list, binary}
  defp string_list(buffer, 0, acc) when is_binary(buffer) do
    {Enum.reverse(acc), buffer}
  end

  @spec string_list(binary, integer, list) :: {list, binary}
  defp string_list(buffer, length, acc) when is_binary(buffer) do
    {string, rest} = string(buffer)
    string_list(rest, length - 1, [string | acc])
  end

  @spec string_multimap(binary) :: {map, binary}
  def string_multimap(<<length::16, buffer::binary>>) do
    string_multimap(buffer, length, %{})
  end

  @spec string_multimap(binary, 0, map) :: {map, binary}
  defp string_multimap(binary, 0, multimap) do
    {multimap, binary}
  end

  @spec string_multimap(binary, 0, map) :: {map, binary}
  defp string_multimap(binary, length, acc) do
    {key_string, rest} = string(binary)
    {value_string_list, rest} = string_list(rest)
    string_multimap(rest, length - 1, Map.put(acc, :"#{key_string}", value_string_list))
  end

  @spec bytes(binary) :: {nil, binary}
  defp bytes(<<255, 255, 255, 255, buffer::binary>>) do
    {nil, buffer}
  end

  @spec bytes(binary) :: {String.t, binary}
  defp bytes(<<size::32, value::size(size)-bytes, buffer::binary>>) do
    {value, buffer}
  end

  @spec inet(binary, map) :: {tuple, integer, binary}
  def inet(<<length::8, address::size(length)-binary, port::32, buffer::binary>>, opts \\ %{}) do
    {data(:inet, address, opts), port, buffer}
  end

  # helper functions for the streamed_rows function
  @spec reverse_or_compute(list, nil, atom) :: list
  defp reverse_or_compute(acc, nil, _) when is_list(acc) do
    :lists.reverse(acc)
  end

  @spec reverse_or_compute(term, tuple, atom) :: Compute.t
  defp reverse_or_compute(acc, function, result_of) do
    Compute.create(acc, function, result_of)
  end

  @spec row_compute(nil, list, list) :: list
  defp row_compute(nil, row, acc) do
    [row | acc]
  end

  @spec row_compute(tuple, list, term) :: term
  defp row_compute({mod, func}, row, acc) do
    apply(mod, func, [row, acc]) # this do edge_computing on the row with the prev acc.
    # NOTE: the result of your function will be the acc for the following(next) row, eg your acc could be the sum of all your prev rows for a given column,
    # or it could be anything you want. (audit tools addons)
  end

  # this enable extra_args
  @spec row_compute(tuple, list, term) :: term
  defp row_compute({mod, func, extra_args}, row, acc) do
    apply(mod, func, extra_args ++ [row, acc])
  end


end
