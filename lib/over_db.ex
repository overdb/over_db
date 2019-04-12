defmodule OverDB do
  alias OverDB.Engine.{Helper, Batcher, Reporter, Sender, Receiver, Preparer}
  alias OverDB.Ring.Monitor
  require Logger
  @moduledoc """

  Documentation for OverDB.
  docs, OverDB ENGINE APIs and APP API (WIP) WORK IN PROGRESS.

  """

  # this is testing for purpose only, a supervision_tree will follow.
  @spec start_simple_topology(atom,atom, integer) :: atom
  def start_simple_topology(otp_app, data_center, sleep \\ 20000) do
    # GenServers per otp_app ------------
    Monitor.start_link(otp_app: otp_app)
    Preparer.start_link(otp_app: otp_app)
    # -----------------------------------
    Process.sleep(sleep)
    reporter_per_shard = Application.get_env(:over_db, otp_app)[:__REPORTERS_PER_SHARD__]
    loggeds = Application.get_env(:over_db, otp_app)[:__LOGGED_PER_SHARD__]
    unloggeds = Application.get_env(:over_db, otp_app)[:__UNLOGGED_PER_SHARD__]
    counters = Application.get_env(:over_db, otp_app)[:__COUNTER_PER_SHARD__]
    conns = Application.get_env(:over_db, otp_app)[:__CONNS_PER_SHARD__]
    priority = Application.get_env(:over_db, otp_app)[:__RECEIVER_PRIORITY__]
    nodes = Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__][data_center]
    for {address, port} <- nodes do
      shards = FastGlobal.get(:"#{otp_app}_#{address}")[:shards]
      if shards do
        if loggeds do
          for conn <- 1..conns, logged <- 1..loggeds, shard <- 0..shards do
            args = [type: :logged, address: address, conn: conn, num: logged, shard: shard, reporter_per_shard: reporter_per_shard, otp_app: otp_app]
            Batcher.start_link(args)
          end
        end
        if unloggeds do
          for conn <- 1..conns, unlogged <- 1..unloggeds, shard <- 0..shards do
            args = [type: :unlogged, address: address, conn: conn, num: unlogged, shard: shard, reporter_per_shard: reporter_per_shard,otp_app: otp_app]
            Batcher.start_link(args)
          end
        end
        if counters do
          for conn <- 1..conns, counter <- 1..counters, shard <- 0..shards do
            args = [type: :counter, address: address, conn: conn, num: counter, shard: shard, reporter_per_shard: reporter_per_shard, otp_app: otp_app]
            Batcher.start_link(args)
          end
        end
        # reporters and senders
        for {partition, range}  <- Helper.gen_stream_ids(reporter_per_shard), conn <- 1..conns, shard <- 0..shards  do
          args = [otp_app: otp_app, conn: conn, shard: shard, range: range, partition: partition, logged: loggeds, unlogged: unloggeds, counter: counters, address: address]
          Reporter.start_link(args)
          args = [otp_app: otp_app, conn: conn, shard: shard, address: address, partition: partition]
          Sender.start_link(args)
        end
        for conn <- 1..conns, shard <- 0..shards do
          args = [otp_app: otp_app, conn: conn, shard: shard, address: address, port: port, priority: priority]
          Receiver.start_link(args)
        end
        :ok
      else
        Logger.error("Make sure ScyllaDB #{address} is Alive")
        {:error, :ring_missing}
      end
    end

  end

  @spec get_reporters_ranges(integer) :: list
  def get_reporters_ranges(reporter_per_shard) do
    Helper.gen_stream_ids(reporter_per_shard)
  end

end
