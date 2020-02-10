defmodule OverDB.Engine.Sender do

  use GenStage
  require Logger
  alias OverDB.Engine.Helper


  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    otp_app = args[:otp_app]
    address = args[:address]
    shard = args[:shard]
    conn = args[:conn]
    partition = args[:partition]
    name = :"#{otp_app}_#{address}_#{shard}_#{conn}_s#{partition}"
    state = Keyword.put(args, :name, name)
    GenStage.start_link(__MODULE__, state, name: name)
  end

  @spec init(Keyword.t) :: tuple
  def init(state) do
    otp_app = state[:otp_app]
    address = state[:address]
    shard = state[:shard]
    conn = state[:conn]
    partition = state[:partition]
    socket_key = :"#{otp_app}_#{address}_#{shard}_#{conn}"
    Process.put(:name, state[:name])
    Process.put(:socket_key, socket_key)
    {:consumer, nil, subscribe_to: [:"#{socket_key}_r#{partition}"]}
  end

  @spec handle_subscribe(atom, list | tuple, tuple, port) :: tuple
  def handle_subscribe(:producer, _, _from, state) do
    Process.send_after(self(), :new_socket, 1000)
    Logger.info("I'm #{Process.get(:name)} got subscribed_to Reporter")
    {:automatic, state}
  end

  @spec handle_info(tuple, port | nil) :: tuple
  def handle_info(:new_socket, _state) do
    socket = FastGlobal.get(Process.get(:socket_key))
    state =
      cond do
        is_nil(socket) ->
          Process.send_after(self(), :new_socket, 1000)
          nil
        true ->
          :erlang.list_to_port(socket)
      end
    {:noreply, [], state}
    # we dont know if the socket if nil or not. as we will ask for new socket only if send request failed or after the init.
    # for now the Reciever is GenServer as we dont want it to subscribe to a lot of senders for no reason.
    # # TODO: check if thousands of subscribation is a bottleneck on reciever, if not, then we can move Receiver to GenStage and
    # having it dispatching events to reporters . # TODO: check the latency of dispatching..
  end

  @spec handle_events(list, tuple, port) :: tuple
  def handle_events(payloads, from, socket) do
    send_me(payloads, socket, from, [])
    # if the result of send_me is false, mean our socket is closed. we don't have to worry as we are already in loop looking for new_socket
    # anyway it may be helpful in future for socket_status metrics.
    {:noreply, [], socket}
  end


  @spec send_me(list, port, tuple, boolean) :: boolean
  defp send_me([{payload, stream_id, ids} | payloads], socket, from, _acc) do
    send? = send_and_tell(socket_send(socket, payload), ids, from, stream_id)
    send_me(payloads, socket, from, send?)
  end

  @spec send_me(list, port, tuple, boolean) :: boolean
  defp send_me([], _, _, acc) do
    acc
  end

  @spec send_and_tell(atom, list, tuple, integer) :: boolean
  defp send_and_tell(:ok, ids, _from, _stream_id) do
    Helper.tell_workers(ids, :send?, :ok)
    true
  end

  @spec send_and_tell(tuple, list, tuple, integer) :: boolean
  defp send_and_tell(err, ids, {pid, _}, stream_id) do
    Helper.tell_workers(ids, :send?, err)
    GenStage.cast(pid, {:drop, stream_id})
    false
  end
  @spec socket_send(port, binary) :: atom | tuple
  defp socket_send(socket, payload) when is_port(socket) do
    case :gen_tcp.send(socket, payload) do
      :ok -> :ok
      err? ->
        Process.send_after(self(), :new_socket, 500) # we are giving 500 ms for a new conn to be established by the receiver.
        err?
    end
  end

  @spec socket_send(term, term) :: tuple
  defp socket_send(_, _) do
    send(self(), :new_socket)
    {:error, :closed}
  end

end
