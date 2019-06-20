defmodule OverDB.Builder.Executor.Execute do

  alias OverDB.Ring.Shard
  alias OverDB.Builder.Cql
  alias OverDB.Protocol.V4.Frames.Responses.Result.Prepared
  alias OverDB.Protocol.V4.Frames.Requests.{Query, Execute, Prepare}

  # all in_batch(insert/update/delete) DML queries are routed to the right shard. (becasue they contain all the values for partition key columns)
  def in_batch(%{prepare?: false, send_cast?: send_cast?, opts: opts, ref: ref, response_to: response_to, rf: rf} = query, conn, b_type, b_num, ring) do
    {type, pk, values, cql} = Cql.build(query)
    query_in_batch =
      Query.create(cql, values)
      |> Query.push(opts)
    pid = Shard.lookup_shard(ring, pk, rf)
    |> Shard.lookup_batcher(conn, b_type, b_num)
    |> alive?()
    value = {:execute, IO.iodata_length(query_in_batch), response_to, ref, query_in_batch}
    query_state = query_state?(type, query, query[:assign], opts)
    {send_cast?(send_cast?, pid, value), ref, query_state}
  end

  def in_batch(%{prepare?: true, send_cast?: send_cast?, opts: opts, ref: ref, response_to: response_to, rf: rf} = query, conn, b_type, b_num, ring) do
    {type, pk, values, cql} = Cql.build(query)
    pid = Shard.lookup_shard(ring, pk, rf)
    |> Shard.lookup_batcher(conn, b_type, b_num)
    |> alive?()
    {query_state, value} = prepared?(:push, type, query, cql, values, response_to, ref, opts, ring)
    {send_cast?(send_cast?, pid, value), ref, query_state}
  end

  # alone functions can be used on all queries to get executed in alone request,
  # mostly it will be used for read(select) queries, therefore the skip metadata should be true in case you want to
  # decode the stream_buffers on the fly and get(rows or compute result right away), otherwise keep it as default, but you have to reassamble the full_buffer
  # from the stream_buffers and only then you can decode the full_buffer and get the full page response containing all the rows.
  def alone(%{prepare?: false, send_cast?: send_cast?, opts: opts, ref: ref, response_to: response_to, rf: rf} = query, conn, ring) do
    skip_metadata? = query[:stream] || false
    {type, pk, values, cql} = Cql.build(query)
    encoded_query =
      Query.create(cql, values)
      |> Query.push_alone(Map.put_new(opts, :skip_metadata?, skip_metadata?))
    pid = Shard.lookup_shard(ring, pk, rf)
    |> Shard.lookup_reporter(conn)
    |> alive?()
    value = {:execute, IO.iodata_length(encoded_query), response_to, ref, encoded_query}
    query_state = query_state?(type, query, query[:assign], opts)
    {send_cast?(send_cast?, pid, value), ref, query_state}
  end

  # Edited: you dont have to pass anything anymore as the prepare? true responses should always not have metadata in thier buffers,
  # this will make it easy to handle it in stream functions even if it got executed as normal query, that's why we are forcing the skip_metadata flag to true.
  def alone(%{prepare?: true, send_cast?: send_cast?, opts: opts, ref: ref, response_to: response_to, rf: rf} = query, conn, ring) do
    {type, pk, values, cql} = Cql.build(query)
    pid = Shard.lookup_shard(ring, pk, rf)
    |> Shard.lookup_reporter(conn)
    |> alive?()
    {query_state, value} = prepared?(:push_alone ,type, query, cql, values, response_to, ref, Map.put_new(opts, :skip_metadata?, true), ring)
    {send_cast?(send_cast?, pid, value), ref, query_state}
  end


  # NOTE: this feature in the roadmap and not ready yet.
  # range queries are always stream true type whether it's prepared or not.
  defp range(%{prepare?: true, send_cast?: send_cast?, opts: opts, ref: ref, response_to: response_to, rf: rf} = query,conn, ring) do
    {type, pk, values, cql} = Cql.build(query)
    pid = Shard.lookup_shard(ring, pk, rf)
    |> Shard.lookup_reporter(conn)
    |> alive?()
    {query_state, value} = prepared?(:push_alone ,type, query, cql, values, response_to, ref, Map.put_new(opts, :skip_metadata?, true), ring)
    {send_cast?(send_cast?, pid, value), ref, query_state}
  end

  # this feature in the roadmap.
  defp range(%{prepare?: false, send_cast?: send_cast?, opts: opts, ref: ref, response_to: response_to, rf: rf} = query,conn, ring) do
    {type, pk, values, cql} = Cql.build(query)
    encoded_query =
      Query.create(cql, values)
      |> Query.push_alone(Map.put_new(opts, :skip_metadata?, true))
    pid = Shard.lookup_shard(ring, pk, rf)
    |> Shard.lookup_reporter(conn)
    |> alive?()
    value = {:execute, IO.iodata_length(encoded_query), response_to, ref, encoded_query}
    query_state = query_state?(type, query, query[:assign], opts)
    {send_cast?(send_cast?, pid, value), ref, query_state}
  end

  defp prepared?(execute?, type, query, cql, values, response_to, ref, opts, ring) do
    prepared? = FastGlobal.get(cql)
    if prepared? do
      prepared = %{prepared? | values: values} |> execute(Execute, execute?, opts)
      query_state = query_state?(type, prepared?, query[:assign], opts)
      {query_state, {:execute,IO.iodata_length(prepared), response_to, ref, prepared}}
    else
      encoded_query = Query.create(cql, values) |> execute(Query, execute?, opts)
      prepare_request = Prepare.push(cql)
      GenServer.cast(ring.preparer?(), {:prepare, cql, prepare_request})
      query_state = query_state?(type, query, query[:assign], opts)
      {query_state, {:execute, IO.iodata_length(encoded_query), response_to, ref, encoded_query}}
    end
  end

  defp execute(value, mod, execute?, opts) do
    apply(mod, execute?, [value, opts])
  end

  defp send_cast?(_, {false, pid}, _) do
    pid
  end

  defp send_cast?(:cast, pid, value) do
    GenServer.cast(pid, value)
  end

  defp send_cast?(:send, pid, value) do
    send(pid, value) # add try catch
  end

  @spec alive?(atom) :: boolean
  defp alive?(atom) when is_atom(atom) do
    whereis = Process.whereis(atom)
    if whereis do
      if Process.alive?(whereis) do
        atom
      else
        {false, atom}
      end
    else
      {false, atom}
    end
  end


  defp query_state?(:stream, %Prepared{result_metadata: result_metadata}, assign, opts) do
    %{type: :stream ,result_metadata: result_metadata, prepared: true, opts: Map.drop(opts, [:skip_metadata?])}
    |> assign(assign)
  end

  defp query_state?(:stream, %{select: select, schema: schema, target: target}, assign, opts) do
    select = if opts[:sort_select?], do: Enum.sort(select), else: select
    result_metadata = result_metadata_generator(select, schema, target)
    %{type: :stream ,result_metadata: result_metadata, prepared: false, opts: Map.drop(opts, [:skip_metadata?])}
    |> assign(assign)
  end

  defp query_state?(type, _, assign, opts) do
    %{type: type, opts: Map.drop(opts, [:skip_metadata?])}
    |> assign(assign)
  end

  defp assign(query_state, nil) do
    query_state
  end

  defp assign(query_state, assign) do
    assign |> Enum.into(query_state)
  end

  defp result_metadata_generator([], schema, target) do
    [keyspace, table] = String.split(target, ["."])
    for {name, %{type: type, opts: _}} <- schema.__struct__.columns do
      {keyspace, table, name, type}
    end
  end

  defp result_metadata_generator(select, schema, target) do
    [keyspace, table] = String.split(target, ["."])
    columns = schema.__struct__.columns
    for column_name <- select do
      type = columns[column_name][:type]
      {keyspace, table, column_name, type}
    end
  end

end
