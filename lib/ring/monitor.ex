defmodule OverDB.Ring.Monitor do

  alias OverDB.Ring.Helper
  require Logger
  use GenServer

  def start_link(args) do
    name = :"#{args[:otp_app]}_ring_monitor"
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(state) do
    send(self(), :all)
    {:ok, state}
  end

  # # TODO: add integrity check to the ranges
  def handle_info(:all, state) do
    otp_app = state[:otp_app]
    module = Application.get_env(:over_db, otp_app)[:__RING__]
    {dead, ranges} = Helper.get_all_ranges(otp_app)
    if dead != [] do
      Logger.warn("dead scylla node(s): #{inspect dead}, please make sure all of them are started probably, and then recompile chronicle again")
    end
    Helper.build_ring(ranges, module, otp_app)
    {:noreply, state}
  end

  # NOTE: the monitor is WIP
  # it should have full control over the data_centers(for now only dc1 is supported) and be able to communicate with all the cluster_managers
  # to dynamicly add or remove nodes as well rebuilding the picture of ring while registering/listening for events(up/down, add/remove),
  # and we should not forget working on config.exs runtime idea to update the __DATA_CENTERS__ in both(runtime, put_env) with the added/removed nodes,
  # this way if the application crashed with dynamicly added/remove scylla nodes, then it will rebuild ring with the recent nodes.
end
