defmodule OverDB.Engine.Batcher do

  use GenStage

  require Logger
  alias OverDB.Engine.Helper

  @batch_size 5000 # this is the default batch threshold size in bytes as defined in scylla.yaml
  @interval 500 # in ms

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    b_type = args[:type]
    num = args[:num]
    conn = args[:conn]
    shard = args[:shard]
    address = args[:address]
    otp_app = args[:otp_app]
    rps = args[:reporter_per_shard]
    interval = args[:interval] || @interval
    b_size = args[:batch_size] || @batch_size
    name = :"#{otp_app}_#{address}_#{shard}_#{conn}_#{Helper.b_type?(b_type)}#{num}"
    GenStage.start_link(__MODULE__, %{name: name, b_info: {b_type, b_size}, interval: interval, rps: rps}, name: name)
  end

  @spec init(map) :: tuple
  def init(%{rps: rps, name: name, b_info: b_info, interval: interval}) do
    Process.put(:name, name)
    dispatcher = {GenStage.PartitionDispatcher, partitions: 1..rps, hash: fn e -> {e, :rand.uniform(rps)} end}
    Process.send_after(self(), {:tick, interval}, interval)
    {:producer, {b_info, 0, [], []}, dispatcher: dispatcher}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("I'm #{Process.get(:name)} got subscribed_to Reporter")
    {:automatic, state}
  end

  @spec handle_demand(integer, map) :: tuple
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  @spec handle_info(atom, map) :: tuple
  def handle_info({:tick, interval}, {{b_type, _b_size} = b_info, c_size, queries, w_ids}) when c_size != 0 do
    Process.send_after(self(), {:tick, interval}, interval)
    {:noreply, [{b_type, queries, w_ids}], {b_info, 0, [], []}}
  end

  @spec handle_info(atom, map) :: tuple
  def handle_info({:tick, interval}, state) do
    Process.send_after(self(), {:tick, interval}, interval)
    {:noreply, [], state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_info({:execute, q_size, pid, ref, encoded_query}, {{_, b_size} = b_info, c_size, queries, w_ids}) when c_size+q_size < b_size do
    {:noreply, [], {b_info, c_size+q_size, [encoded_query|queries], [{pid, ref, 0}| w_ids]}}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_info({:execute, q_size, pid, ref, encoded_query}, {{b_type, b_size} = b_info, c_size, queries, w_ids}) when c_size+q_size >= b_size do
    event = {b_type, [encoded_query|queries], [{pid, ref, 0}| w_ids]}
    {:noreply, [event], {b_info, 0, [], []}}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:execute, q_size, pid, ref, encoded_query}, {{_, b_size} = b_info, c_size, queries, w_ids}) when c_size+q_size < b_size do
    {:noreply, [], {b_info, c_size+q_size, [encoded_query|queries], [{pid, ref, 1}| w_ids]}}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:execute, q_size, pid, ref, encoded_query}, {{b_type, b_size} = b_info, c_size, queries, w_ids}) when c_size+q_size >= b_size do
    event = {b_type, [encoded_query|queries], [{pid, ref, 1}| w_ids]}
    {:noreply, [event], {b_info, 0, [], []}}
  end

end
