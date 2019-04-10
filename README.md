# OverDB

**TODO: Add More description AND DOCS**


## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `over_db` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:over_db, "~> 0.1.0"}
  ]
end
```

Make sure to run the following command `mix deps.get`


## Configuration

```elixir

config :over_db, :my_app, # or MyApp
  __USERNAME__: "username", # CQL username for auth # auth not completely ready
  __PASSWORD__: "password", # CQL password for auth # auth not completely ready
  __DATA_CENTERS__: [
    dc1: [
      {'127.0.0.1', 9042},
      # this was a development node, but keep in mind it's not recommend to run scylla_node and OverDB node in same env.
    ],
  ],
  __RING__: :my_app_ring, # or whatever modulename you want to be the ring.
  __RECEIVER_PRIORITY__: :normal, # by default is normal, but we recommend setting it to :high with more reporters
  __REPORTERS_PER_SHARD__: 4,
  __CONNS_PER_SHARD__: 1,
  __LOGGED_PER_SHARD__: 1,
  __UNLOGGED_PER_SHARD__: 1,
  __COUNTER_PER_SHARD__: 1

  # Diagram and more details will follow up soon, stay tuned

```
## Setup Keyspace(s)

```elixir

defmodule KeyspaceModule do

  use OverDB.Builder.Keyspace,
    otp_apps: [:my_app]

  keyspace :keyspace_name  do
    replication_factor 1 # for now make sure to added here and .. will work on better keyspace structure
    with_options [
      replication: "{'class' : 'SimpleStrategy', 'replication_factor': 1}" # here
    ]
  end

end

```
## Setup Table(s)

```elixir

defmodule TableModule do

  use OverDB.Builder.Table,
    keyspaces: [
      {KeyspaceModule, [:my_app]}
    ]

  table :table_name do
    column :column_name, :column_type
    # column :new_column_name, :column_type
    # add as many columns as you want, the column_types are CQL V4
    # check https://docs.scylladb.com/getting-started/types/
    # example INT you should write it as :int
    # or map as {:map, :key_type, :value_type}
    # more details regarding the types will follow
    partition_key [:column_name] # CQL Partition key
    cluster_columns [:cluster_column_a, :cluster_column_b] # CQL Clustering keys
    # We will make sure to add all the details regarding other options (Ordering)..
  end

end

```
## Setup Executor

```elixir

defmodule ExecutorModule do

  use OverDB.Builder.Executor,
    otp_app: :my_app,
    ring: :my_app_ring

end

```

## Setup Worker

```elixir

defmodule WorkerTest do

  use OverDB.Worker,
    executor: ExecutorModule

  use GenServer # or GenStage

  # this is just a GenServer example

  @spec start_link(map) :: tuple
  def start_link(name) do
    GenServer.start_link(__MODULE__, %{name: name}, name: name)
  end

  @spec init(map) :: tuple
  def init(state) do

    # # NOTE: the example is
    # insert in logged batch example using the query_builder
    # {KeySpaceModule, TableModule} |> insert([column_name: column_value, etc]) |> Execute.logged() # which mean is going to send it to logged batcher
    # insert in unlogged/counter batch , as simple as Execute.unlogged() or Execute.counter() for counter queries
    # insert as normal query Executor.query()
    # select query example
    # {KeySpaceModule, TableModule} |> select([] # or [:column_name, etc] )
    # |> where(column_name: [=: partition_value], timestamp_column?: [>=: value])
    # |> Execute.logged() # which mean is going to send it to logged batcher.
    # We will add fully detailed examples about update/delete/insert/select/range(WIP)
    {:ok, state}
  end

  def handle_cast({:send?, _query_ref, status}, state) do
    # because the engine is async, there is a big chance that you may recv responses before even acknowledge the send? :)
    # keep in mind this still very helpful to handle the queries which didn't made it to the socket,
    # or you can simply add the :send? status to the query_state if needed.
    {:noreply, state}
  end

  def handle_cast({:full, _query_ref, buffer}, query_state) do
    # here you will receive all kinds of responses.      
    # handle the full buffer as whatever you like. and then drop state if you want.
    end
    _response = Protocol.decode_full(buffer, query_state)
    {:noreply, %{}}
  end

  def handle_cast({:start, query_ref, buffer}, query_state) do
    case Protocol.decode_start(buffer, query_state) do
      {rows, query_state} ->
        # handle rows stream as whatever you like.
        # something I didn't talk about yet, which is the
        # the most powerful feature in OverDB, %Compute{} result instead of rows.
        # don't worry will explain it later this week.
        {:noreply, query_state}
      %Ignore{state: query_state} ->
        # you are not suppose to do anything here except returning the new query_state
        {:noreply, query_state}
    end
  end

  def handle_cast({:stream, query_ref, buffer}, query_state) do
    case Protocol.decode_stream(buffer, query_state) do
      {rows, query_state} ->
        # handle rows stream as whatever you like.
        {:noreply, query_state}
      %Ignore{state: query_state} ->
        # you are not suppose to do anything here except returning the new query_state
        {:noreply, query_state}
    end
  end

  def handle_cast({:end, query_ref, buffer}, query_state) do
    case Protocol.decode_end(buffer, query_state) do
      {rows, query_state} ->
        # handle the last stream of the rows.
        {:noreply, %{}}
      response ->
        # this is %Rows{} response. which is the result of select queries with prepare? false.
        {:noreply, %{}}
    end
  end

end

```

## Run

  Make sure you already have ScyllaDB running, because
  at compile time will create your Keyspaces and Tables in ScyllaDB Cluster.


```elixir

  # (otp_app, data_center_key, sleep_time) more details and support for network_topology(google Cassandra replication strategies) will follow
  OverDB.start_simple_topology(:my_app, :dc1, 20000)

  # start your predefined workers..

```
## Notes

OverDB is the first Distrbuited/Graph Multi Model Database which benefits from the Shard Awareness feature.
Keep in mind this is still WORK IN PROGRESS Project, a lot to be done, for now is intended ONLY for powerusers.
anyway We are so excited to disclose more details with you ASAP. (in near future).

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/over_db](https://hexdocs.pm/over_db).
