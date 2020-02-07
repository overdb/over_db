defmodule OverDB.Ring.Helper do

  alias OverDB.Connection
  alias OverDB.Protocol.V4.Frames.Responses.Result.Rows
  alias OverDB.Protocol.V4.Frames.Requests.Query
  alias OverDB.Ring
  @min_token -9223372036854775808
  @max_token 9223372036854775807


  # TODO: adding error handling. dynamicly starting connection with specific IPs.
  # refactoring the func with error handling (WIP).
  @spec get_all_ranges(atom) :: list
  def get_all_ranges(otp_app) do
    {dead, conns} = Connection.start_all(otp_app)
    payload =
      Query.create("SELECT rpc_address, tokens FROM system.local")
      |> Query.new(%{})
    {dead, vnodes} = pull_ranges_from_conns(conns, payload, {dead, []})
    ranges = List.flatten(vnodes) |> :lists.usort() |> ranges()
    {dead, ranges}
  end

  @spec build_ring(list, atom, atom) :: atom
  def build_ring(ranges, module, otp_app) do
    ranges |> build_scylla_ring(module, otp_app) |> build_local_ring(otp_app)
  end

  @spec build_scylla_ring(list, atom, atom) :: list
  def build_scylla_ring([], _, _) do
    []
  end

  @spec build_scylla_ring(list, atom, atom) :: list
  def build_scylla_ring(ranges, module, otp_app) do
    Ring.gen_functions(ranges, module, otp_app)
  end

  @spec build_local_ring(list, atom) :: atom
  def build_local_ring([], _) do
    []
  end

  @spec build_local_ring(list, atom) :: atom
  def build_local_ring(ranges, otp_app) do
    nodes = Enum.reduce(ranges, [], fn ({_, node_id}, acc) -> [node_id | acc] end) |> Enum.uniq()
    conns =  Application.get_env(:over_db, otp_app)[:__CONNS_PER_SHARD__]
    reporters =  Application.get_env(:over_db, otp_app)[:__REPORTERS_PER_SHARD__]
    logged =  Application.get_env(:over_db, otp_app)[:__LOGGED_PER_SHARD__]
    unlogged =  Application.get_env(:over_db, otp_app)[:__UNLOGGED_PER_SHARD__]
    counter =  Application.get_env(:over_db, otp_app)[:__COUNTER_PER_SHARD__]
    build_local_ring(otp_app,nodes, conns, reporters, logged, unlogged, counter)
  end


  defp build_local_ring(otp_app,nodes, conns, reporters, logged, unlogged, counter) do
    reporters_ranges = OverDB.Engine.Helper.gen_stream_ids(reporters)
    put_nodes(otp_app, nodes, conns, reporters_ranges, logged, unlogged, counter, [])
    |> Enum.each(fn({node_id, map}) -> FastGlobal.put(node_id, map) end)
  end

  defp put_nodes(_,[], _, _, _, _, _, nodes) do
    nodes
  end

  defp put_nodes(otp_app, [{{a,b,c,d}, nr, _} | t], conns, reporters, logged, unlogged, counter, acc) do
    shards = nr-1
    ring_key = :"#{otp_app}_#{a}.#{b}.#{c}.#{d}"
    node = {ring_key, put_shards(ring_key, shards, conns, reporters, logged, unlogged, counter, %{shards: shards})}
    put_nodes(otp_app,t, conns, reporters, logged, unlogged, counter, [node | acc])
  end


  defp put_shards(_, -1, _, _, _, _, _, acc) do
    acc
  end

  defp put_shards(ring_key, shard, conns, reporters, logged, unlogged, counter, acc) do
    shard_map = put_conns(:"#{ring_key}_#{shard}", conns, reporters, logged, unlogged, counter, %{})
    acc = Map.put(acc, shard, shard_map)
    put_shards(ring_key, shard-1, conns, reporters, logged, unlogged, counter, acc)
  end

  defp put_conns(_, 0, _, _, _, _, acc) do
    acc
  end

  defp put_conns(ring_key, conn, reporters, logged, unlogged, counter, acc) do
    reporters_batchers_map = put_reporters_batchers(:"#{ring_key}_#{conn}", reporters, logged, unlogged, counter)
    put_conns(ring_key, conn-1, reporters, logged, unlogged, counter, Map.put(acc, conn, reporters_batchers_map))
  end

  defp put_reporters_batchers(ring_key, reporters, logged, unlogged, counter) do
    reporters_map =
      for {num, range} <- reporters do
        for stream_id <- range do
          {stream_id, :"#{ring_key}_r#{num}"}
        end
      end |> List.flatten()
    batcher?(reporters_map, logged, :l, ring_key) |> batcher?(unlogged, :u, ring_key) |> batcher?(counter, :c, ring_key)
    |> Enum.into(%{})
  end

  defp batcher?(map, b_num, b_letter, ring_key) do
    if b_num do
      b_map =
        for b <- 1..b_num do
          {b, :"#{ring_key}_#{b_letter}#{b}"}
        end |> Enum.into(%{})
      [{b_letter, b_map} | map]
    else
      map
    end
  end

  defp ranges([{_, host_id} | _] = ring) do
    {_, last_host_id} = List.last(ring)
    [{@min_token, host_id} | ring] ++ [{@max_token, last_host_id}]
    |> continuous_range()
  end

  defp ranges([]) do
    []
  end

  defp continuous_range([{min_token, host_id} | left]) do

    continuous_range(left, [], {{min_token, nil}, host_id})
  end

  defp continuous_range([{token, _}], acc, {{a, nil}, prev_host_id}) do
    [{{a, token}, prev_host_id} | acc] |> Enum.reverse()
  end


  defp continuous_range([{_, host_id} | left], acc, {{_, nil}, prev_host_id} = prev) when prev_host_id == host_id do
    continuous_range(left, acc, prev)
  end

  defp continuous_range([{token, host_id} | left],acc, {{a, nil}, prev_host_id}) when prev_host_id != host_id do
    continuous_range(left, [ {{a, token}, prev_host_id} | acc], {{token, nil}, host_id})
  end

  defp pull_ranges_from_conns([%Connection{socket: socket, address: address, port: port, data_center: dc, options:
   %{SCYLLA_NR_SHARDS: [nr_shards],SCYLLA_SHARDING_IGNORE_MSB: [msb_ignore]}} | conns], payload, {dead, vnodes}) do
     nr_shards = String.to_integer(nr_shards)
     msb_ignore = String.to_integer(msb_ignore)
    response = Connection.push(payload, socket) |> Connection.sync_recv()
    |> OverDB.Protocol.decode_frame(%{set: :list})
    acc =
      case response do
        %Rows{page: [page]} ->
          [rpc_address: rpc_address, tokens: tokens] = page
          result = Enum.map(tokens, fn(token) -> {String.to_integer(token), {rpc_address, nr_shards, msb_ignore}} end)
          {dead, [result|vnodes]}
        err? ->
          error = {err?, {dc, address, port}}
          {{[error|dead]}, vnodes}
      end
    :gen_tcp.close(socket)
    pull_ranges_from_conns(conns, payload, acc)
  end

  defp pull_ranges_from_conns([], _, acc) do
    acc
  end

end
