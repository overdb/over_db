defmodule OverDB.Ring.Monitor do

  alias OverDB.Ring.Helper

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
    Helper.get_all_ranges(otp_app)
    |> Helper.build_ring(module, otp_app)
    {:noreply, state}
  end

  
end
