defmodule OverDB.Engine.Reporter do

  use GenStage
  alias OverDB.Engine.Helper
  alias OverDB.Protocol.V4.Frames.Requests.Batch
  require Logger

  # TODO: : refactor the engine to have thousands of reporters per given stream_ids,
  # TODO: we should move the expensive work from start_link and init/handle_cou to handle_countiune()
  @spec start_link(Keyword.t) :: tuple
  def start_link(state) do
    otp_app = state[:otp_app]
    address = state[:address]
    shard = state[:shard]
    conn = state[:conn]
    logged = state[:logged]
    unlogged = state[:unlogged]
    counter = state[:counter]
    partition = state[:partition]
    range = state[:range]
    name = :"#{otp_app}_#{address}_#{shard}_#{conn}_r#{partition}"
    subscribe_to? = :"#{otp_app}_#{address}_#{shard}_#{conn}"
    state = %{workers: %{},shard: shard, partition: partition, otp_app: otp_app, stream_ids: range, name: name,conn: conn, subscribe_to?: subscribe_to?, logged: logged, unlogged: unlogged, counter: counter}
    GenStage.start_link(__MODULE__, state, name: name)
  end


  @spec init(map) :: tuple
  def init(%{stream_ids: range ,partition: partition,  name: name,conn: conn, subscribe_to?: subscribe_to?, shard: shard, otp_app: otp_app} = state) do
    Process.put(:name, name)
    Helper.register_me(shard,partition,conn, name, otp_app) # move this call to handle_coutiune
    subscribe_to = Helper.subscribe_to(subscribe_to?, partition, state[:logged], state[:unlogged], state[:counter])
    state = Map.drop(state, [:otp_app, :shard, :name, :subscribe_to?, :logged, :unlogged, :counter, :conn])
    stream_ids = range |> Enum.into([])
    {:producer_consumer, %{state | stream_ids: stream_ids}, subscribe_to: subscribe_to}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:producer, _, _, state) do
    Logger.info("I'm #{Process.get(:name)} got subscribed_to Batcher")
    {:automatic, state}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:consumer, _, _, state) do
    Logger.info("I'm #{Process.get(:name)} got subscribed_to Sender")
    {:automatic, state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_info({:execute,_size, pid, ref, <<v_f::2-bytes, rest::binary>>}, %{stream_ids: [stream_id | stream_ids], workers: workers} = state) do
    payload = <<v_f::binary, stream_id::16, rest::binary>>
    workers = Map.put(workers, stream_id, [{pid, ref, 0}])
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, stream_ids)
    {:noreply, [{payload, stream_id, [{pid, ref, 0}]}], state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_info({:execute,__size, pid, ref, _}, %{stream_ids: []} = state) do
    send(pid, {:overloaded, ref})
    {:noreply, [], state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:execute,_size, pid, ref, <<v_f::2-bytes, rest::binary>>}, %{stream_ids: [stream_id | stream_ids], workers: workers} = state) do
    payload = <<v_f::binary, stream_id::16, rest::binary>>
    workers = Map.put(workers, stream_id, [{pid, ref, 1}])
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, stream_ids)
    {:noreply, [{payload, stream_id, [{pid, ref, 1}]}], state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:execute,_size, pid, ref, _}, %{stream_ids: []} = state) do
    GenStage.cast(pid, {:overloaded, ref}) # maybe we should forward it to another random reporter. we will do more tests first.
    {:noreply, [], state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:prepare, pid, ref, <<v_f::2-bytes, rest::binary>>, boolean}, %{stream_ids: [stream_id | stream_ids], workers: workers} = state) do
    payload = <<v_f::binary, stream_id::16, rest::binary>>
    workers = Map.put(workers, stream_id, [{pid, ref, 1, boolean}])
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, stream_ids)
    {:noreply, [{payload, stream_id, [{pid, ref, 1}]}], state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_cast({:full, stream_id, buffer}, %{workers: workers, stream_ids: stream_ids} = state) do
    worker_ids = Map.get(workers, stream_id)
    Helper.tell_workers(worker_ids, :full, buffer)
    workers = Map.drop(workers, [stream_id])
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, [stream_id | stream_ids])
    {:noreply, [], state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_cast({:start, stream_id, buffer},  %{workers: workers} = state) do
    worker_ids = Map.get(workers, stream_id)
    Helper.tell_workers(worker_ids, :start, buffer)
    {:noreply, [], state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_cast({:stream, stream_id, buffer},  %{workers: workers} = state) do
    worker_ids = Map.get(workers, stream_id)
    Helper.tell_workers(worker_ids, :stream, buffer)
    {:noreply, [], state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_cast({:end, stream_id, buffer},  %{workers: workers, stream_ids: stream_ids} = state) do
    worker_ids = Map.get(workers, stream_id)
    Helper.tell_workers(worker_ids, :end, buffer)
    workers = Map.drop(workers, [stream_id])
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, [stream_id | stream_ids])
    {:noreply, [], state}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_cast({:drop, stream_id},  %{workers: workers, stream_ids: stream_ids} = state) do
    workers = Map.drop(workers, [stream_id])
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, [stream_id | stream_ids])
    {:noreply, [], state}
  end


  @spec handle_events(list, tuple, map) :: tuple
  def handle_events(events, _from, %{workers: workers, stream_ids: stream_ids} = state) do
    IO.inspect({Process.get(:name), events})
    {payloads, workers, stream_ids} = workers_put(events, workers, stream_ids)
    state = Map.put(state, :workers, workers)
    state = Map.put(state, :stream_ids, stream_ids)
    {:noreply, payloads, state}
  end

  @spec workers_put(list, map, list) :: tuple
  defp workers_put(events, workers, stream_ids) do
    workers_put(events, workers, stream_ids, [])
  end

  @spec workers_put(list, map, list, list) :: tuple
  defp workers_put([{type, batch, ids} | events], workers, [stream_id | stream_ids], acc) do
    workers = Map.put(workers, stream_id, ids)
    # TODO we should add logic here to convert a batch with length 1 to a query instead of batch
    payload = Batch.push(batch, type, stream_id)
    # NOTE: added the ids to be able to let the sender tell the workers that the request have been sent to the socket
    workers_put(events, workers, stream_ids, [{payload, stream_id, ids} | acc])
  end

  @spec workers_put([], map, list, list) :: tuple
  defp workers_put([], workers, stream_ids, payloads) do
    {payloads, workers, stream_ids}
  end

  @spec workers_put(list, map, [], list) :: tuple
  defp workers_put(events, workers, [], acc) do
    drop(events)
    {acc, workers, []}
  end

  @spec drop([]) :: nil
  defp drop([]) do
    nil
  end

  @spec drop(list) :: nil
  defp drop([{_type, _batch, ids} | events]) do
    Helper.tell_workers(ids, :overloaded)
    drop(events)
  end

end
