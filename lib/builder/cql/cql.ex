defmodule OverDB.Builder.Cql do

  alias OverDB.Protocol.V4.Frames.Requests.Encoder
  alias OverDB.Builder.Helper.Murmur
  import String, only: [split: 3, upcase: 1, contains?: 2]

  @spec build(map) :: tuple
  def build(%{cql: _} = query) do
    cql(query)
  end

  @spec build(map) :: tuple
  def build(%{insert: _} = query) do
    insert(query)
  end

  @spec build(map) :: tuple
  def build(%{range: _} = query) do
    range(query)
  end

  @spec build(map) :: tuple
  def build(%{select: _} = query) do
    select(query)
  end

  @spec build(map) :: tuple
  def build(%{update: _} = query) do
    update(query)
  end

  @spec build(map) :: tuple
  def build(%{delete: _} = query) do
    delete(query)
  end

  # TODO:  range function # not part of the API yet.
  @spec range(map) :: tuple
  def range(%{select: _select, range: {:undefined,b}} = query) do
    range(Map.put(query, :range, [<=: b]), b)
  end

  @spec range(map) :: tuple
  def range(%{select: _select, range: {a,:undefined}} = query) do
    range(Map.put(query, :range, [>: a]), a+1)
  end

  @spec range(map) :: tuple
  def range(%{select: _select, range: {a,b}} = query) do
    range(Map.put(query, :range, [>: a, <=: b]), b)
  end

  @spec range(map, integer) :: tuple
  defp range(%{select: select, range: range, target: target, schema: schema} = query, virtual_pk) do
    pk = "token(#{Enum.join(schema.__struct__.partition_key, ",")})"
    range = Enum.join(Enum.map(range, fn({sign, token}) -> "#{pk} #{sign} #{token}" end), " AND ")
    {values, cql} = where(query[:where], schema.__struct__)
    cql = "SELECT #{select_columns(select)} FROM #{target} WHERE #{range}#{String.replace(cql, "WHERE", "AND")}"
    {:stream, virtual_pk, values, cql} # range queries will be forced to :stream type. the virtual_pk enable the exectue engine to find the right shard.
  end

  # Hardcoded CQL function # NOTE: intended for powerusers only.
  @spec cql(map) :: tuple
  defp cql(%{cql: cql, type: type, values: values, pk: pk, schema: schema}) do
    pk = Encoder.compute_partition_key(pk, schema) |> Murmur.create()
    {type, pk, values, cql}
  end

  # select functions
  @spec select(map) :: tuple
  defp select(%{select: select, target: target, schema: schema} = query) when is_list(select) do
    schema = schema.__struct__
    select = Enum.sort(select)
    where = query[:where]
      if where do
        Enum.sort(where)
      else
        nil
      end
    {values, cql} = where(where, schema)
    {limit_value, limit_cql} = limit(query[:limit])
    allow_filtering = allow_filtering(query[:allow_filtering])
    pk? = pk?(where, schema.partition_key)
    pk = get_partition_key(where, schema, pk?)
    cql = "SELECT #{select_columns(select)} FROM #{target}#{cql}" <> "#{limit_cql}" <> "#{allow_filtering}"
    action = if query[:stream] == true or query[:prepare?] == true do # because prepare? = true convert select query to stream, and will force the response to not have metadata.
      :stream
    else
      :select
    end
    {action, pk, values ++ limit_value , cql}
  end

  # insert functions

  @spec insert(map) :: tuple
  defp insert(%{insert: insert, target: target, schema: schema}) when is_list(insert) do
    schema = schema.__struct__
    insert = Enum.sort(insert)
    values = insert(insert, schema)
    pk? = pk?(insert, schema.partition_key)
    pk = get_partition_key(insert, schema, pk?)
    {:insert, pk, values, "INSERT INTO #{target} (#{cols_keys(insert)}) VALUES (#{cols_values(insert)})"}
  end

  @spec insert(list, map) :: list
  defp insert(insert, schema) do
    columns = schema.columns
    for {col, value} <- insert do
      {columns[col][:type], value}
    end
  end

  # Delete function

  @spec delete(map) :: tuple
  defp delete(%{delete: delete, target: target, where: where, schema: schema}) when is_list(delete) do
    schema = schema.__struct__
    delete = Enum.sort(delete)
    {values, cql} = where(where, schema)
    pk? = pk?(where, schema.partition_key)
    pk = get_partition_key(where, schema, pk?)
    {:delete, pk, values, "DELETE#{delete_columns(delete)} FROM #{target}#{cql}"}
  end

  @spec delete_columns(list) :: String.t
  defp delete_columns([]), do: <<>>

  @spec delete_columns(list) :: String.t
  defp delete_columns(cols), do: " " <> select_columns(cols)


  # update functions
  @spec update(map) :: tuple
  defp update(%{update: update, target: target, where: where, schema: schema}) do
    schema = schema.__struct__
    update = Enum.sort(update)
    {where_values, where_cql} = where(where, schema)
    {update_values, update_cql} = update(update, schema)
    values = update_values ++ where_values
    pk? = pk?(where, schema.partition_key)
    pk = get_partition_key(where, schema, pk?)
    cql = "UPDATE #{target} #{update_cql}#{where_cql}"
    case schema.type do
      :normal -> {:update ,pk, values, cql}
      :counter -> {:counter, pk, values, cql}
    end
  end


  @spec update(list, map) :: tuple
  defp update([ {k, [{sign, v}]} | update], schema) do
    {value, col_cql} = update_case(k, v,sign, schema)
    cql = "SET #{col_cql}"
    update(update, schema, {[value], cql})
  end

  @spec update(list, map, tuple) :: tuple
  defp update([], _schema, {acc, cql}) do
    {:lists.reverse(acc), cql}
  end

  @spec update(list, map, tuple) :: tuple
  defp update([{k, [{sign, v}]} | update], schema, {acc, cql}) do
    {value, col_cql} = update_case(k, v,sign, schema)
    cql = cql <> ", " <> col_cql
    update(update, schema, {[value | acc], cql})
  end

  @spec update_case(atom, term, atom, map) :: {tuple, binary} | tuple
  defp update_case(k, v, sign, schema) do
    columns = schema.columns
    case sign do
      := ->
        column = collection_column_parser(k)
        type = columns[column][:type]
        case type do
          {:map, _k_type, v_type} ->
            if contains?("#{k}", ["[", "]"]) do
              {{v_type, v}, "#{k} = ?"}
            else
              { {type, v}, "#{k} = ?"}
            end
          {:set, _i_type} ->
            { {type, v}, "#{k} = ?"}
          {:list, e_type} ->
            if contains?("#{k}", ["[", "]"]) do
              {{e_type, v}, "#{k} = ?"}
            else
              { {type, v}, "#{k} = ?"}
            end
          _ ->
            { {type, v}, "#{k} = ?"}
        end
      :+ ->
        cql = "#{k} = #{k} + ?"
        { {columns[k][:type], v} , cql}
      :- ->
        type = columns[k][:type]
        case type do
          {:map, k_type, _v_type} ->
            {{{:set, k_type }, v}, "#{k} = #{k} - ?"}
          _ ->
            {{type, v}, "#{k} = #{k} - ?"}
        end
      :++ ->
        type = columns[k][:type]
        {{type, v}, "#{k} = ? + #{k}"}
    end
  end




  @spec cols_keys(list) :: String.t
  defp cols_keys(cols) when is_list(cols), do: cols |> Enum.map(fn {k, _} -> k end) |> Enum.join(", ")

  @spec cols_values(list) :: String.t
  defp cols_values(cols) when is_list(cols), do: cols |> Enum.map(fn {_, _} -> "?" end) |> Enum.join(", ")


  @spec where(nil, map) :: tuple
  defp where(nil, _schema) do
    {[], <<>>}
  end

  @spec where(list, map) :: tuple
  defp where([ {k, [{sign, v}]} | where], schema) do
    {value, col_cql} = where_case(k, v,sign, schema)
    sign = upcase("#{sign}")
    cql = " WHERE #{col_cql} #{sign} ?"
    where(where, schema, {[value], cql})
  end

  @spec where(list, map, tuple) :: tuple
  defp where([], _schema, {acc, cql}) do
    {:lists.reverse(acc), cql}
  end

  @spec where(list, map, tuple) :: tuple
  defp where([{k, [{sign, v}]} | where], schema, {acc, cql}) do
    {value, col_cql} = where_case(k, v,sign, schema)
    sign = upcase("#{sign}")
    cql = cql <> " AND " <> col_cql <> " #{sign} " <> "?"
    where(where, schema, {[value | acc], cql})
  end

  @spec where_case(atom, term, atom, map) :: {tuple, binary} | tuple
  defp where_case(k, v, sign, schema) do
    columns = schema.columns
    case "#{k}" do
      <<"in(", rest::binary>> ->
        column_names = split(rest, [" ", ")", ","], trim: true)
        case column_names do
          [column] ->
            type = columns[:"#{column}"][:type]
            { {{:list, type}, v}, column} # edited from in(column)
          [_hd | _tl] ->
            column_names = tuple_statement_sorter(column_names, schema)
            types = types(column_names, columns)
            cql = "(#{Enum.join(column_names, ",")})"
            { {{:list, {:tuple, types}}, v}, cql}
        end
      <<"(", rest::binary>> ->
        column_names = split(rest, [" ", ")", ","], trim: true)
        column_names = tuple_statement_sorter(column_names, schema)
        types = types(column_names, columns)
        cql = "(#{Enum.join(column_names, ",")})"
        if sign == :in do
          { {{:list, {:tuple, types}}, v}, cql}
        else
          { {{:tuple, types}, v}, cql}
        end
      column_name ->
        type = columns[:"#{column_name}"][:type]
        { {type, v}, column_name}
    end
  end

  @spec limit(integer) :: String.t()
  defp limit(limit) when is_integer(limit), do: {[{:int, limit}], " LIMIT ?"}

  @spec limit(term) :: binary
  defp limit(_), do: {[], ""}

  @spec allow_filtering(boolean) :: String.t
  defp allow_filtering(true), do: " ALLOW FILTERING"

  @spec allow_filtering(term) :: binary
  defp allow_filtering(_), do: ""


  @spec select_columns(list) :: String.t
  defp select_columns([]), do: "*"

  @spec select_columns(list) :: String.t
  defp select_columns(cols), do: cols |> Enum.map(fn col -> col end) |> Enum.join(", ")

  @spec tuple_statement_sorter(list, map) :: list
  defp tuple_statement_sorter(column_names, schema) do
    cluster_columns = schema.cluster_columns
    indices =
      for column <- column_names do
        Enum.find_index(cluster_columns, fn(col) -> "#{col}" == column end)
      end
    for index <- Enum.sort(indices) do
      if index do
        Enum.at(cluster_columns, index)
      else
        raise "Tuple statement must only contains clustering_columns"
      end
    end
  end

  @spec types(list,  map) :: atom | tuple
  defp types(column_names, columns) do
    for column <- column_names do
      columns[column][:type]
    end
  end

  @spec collection_column_parser(atom) :: atom
  defp collection_column_parser(column_name) do
    column_string = "#{column_name}"
    if contains?(column_string, ["[", "]"]) do
      [name | _]= split(column_string, ["[", "]"], trim: true)
      :"#{name}"
    else
      column_name
    end
  end

  @spec pk?(nil, list) :: nil
  defp pk?(nil, _pks) do
    nil
  end

  @spec pk?(list, list) :: boolean
  defp pk?(list, pks) do
    Enum.all?(pks, fn pk -> Keyword.has_key?(list, pk) end)
  end

  @spec get_partition_key(list,  map, boolean | nil) :: list
  defp get_partition_key(list, schema, pk?) do
    case pk? do
      true ->
        Encoder.compute_partition_key(list, schema)
        |> Murmur.create()
      false_or_nil -> false_or_nil
    end
  end

end
