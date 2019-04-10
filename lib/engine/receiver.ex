defmodule OverDB.Engine.Receiver do

  use GenServer
  alias OverDB.Connection
  require Logger

  # TODO: upgrade to OTP21 and add handle_continue in the whole engine.

  @spec start_link(Keyword.t) :: tuple
  def start_link(state) do
    otp_app = state[:otp_app]
    address = state[:address]
    port = state[:port]
    shard = state[:shard]
    priority = state[:priority]
    conn = state[:conn]
    r_info = {:"#{otp_app}_#{address}",shard, conn}
    name = :"#{otp_app}_#{address}_#{shard}_#{conn}_receiver"
    state = %{otp_app: otp_app,r_info: r_info, priority: priority, length: 0, active_id: nil, buffer: <<>>,conn: conn, address: address, port: port, shard: shard, name: name}
    GenServer.start_link(__MODULE__, state, name: name)
  end

  @spec init(map) :: tuple
  def init(%{otp_app: otp_app,priority: priority, name: name, address: address, port: port, shard: shard, conn: conn} = state) do
    Process.put(:info?, {address, port, shard})
    Process.flag(:priority, priority)
    case Connection.start(state) do
      %Connection{socket: socket} ->
        # pattern matching with Connection struct is the easiet way to verify if the connection started or not.
        # all the failover logic is going to move to handle_continue as soon as we support it.
        socket_key = :"#{otp_app}_#{address}_#{shard}_#{conn}"
        port_as_list = :erlang.port_to_list(socket)
        FastGlobal.put(socket_key, port_as_list)
        Process.put(:socket_key, socket_key)
        {:ok, Map.drop(state, [:otp_app,:name, :shard, :port, :address, :conn, :priority])}
      _ ->
        Logger.error("Receiver #{name} couldn't establish a connection to address: #{address}, port: #{port}, shard: #{shard}, conn: #{conn}")
        {:ok, state}
    end

  end

  # this handle the buffer from the active socket
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:tcp, _socket, buffer}, %{r_info: r_info, length: length, active_id: active_id, buffer: old_buffer}) do
    {old_buffer, length, active_id}  = streamer(old_buffer <> buffer, length, active_id, r_info)
    {:noreply, %{r_info: r_info, length: length, active_id: active_id, buffer: old_buffer}}
  end

  @spec handle_info(tuple, map) :: tuple
  def handle_info({:tcp_closed, socket}, state) do
    {address, port, shard} = Process.get(:info?)
    conn = Connection.start(%{address: address, port: port, shard: shard})
    case conn do
      %Connection{socket: socket} ->
        socket_key = Process.get(:socket_key)
        FastGlobal.put(socket_key, :erlang.port_to_list(socket))
      _ -> Process.send_after(self(), {:tcp_closed, socket}, 500)
    end
    {:noreply, state}
  end

  #this deal with full response with body, (when left_length = 0 means we are decoding a new buffer)
  @spec streamer(binary, integer, integer, atom) :: tuple
  defp streamer(<<0x84, flags, stream_id::16, opcode, length::32, body::size(length)-bytes, rest::binary>> , 0, _active_id, r_info) do
    stream_me(:full, <<0x84, flags, stream_id::16, opcode, length::32, body::binary>>, stream_id, r_info)
    streamer(rest, 0, nil, r_info) # NOTE: reseted active_id to nil
  end

  # this deal with non full response but include the length
  # NOTE: we forced this function to only start a stream for select(rows) responses.
  @spec streamer(binary, integer, integer, atom) :: tuple
  defp streamer(<<0x84, _flags, stream_id::16, 0x08, length::32, 0x0002::32-signed, rest::binary>> = buffer, 0, _, r_info) do
    stream_me(:start, buffer, stream_id, r_info)
    length = length - (byte_size(rest)+4)
    streamer(<<>>, length, stream_id, r_info)
  end

  @spec streamer(binary, integer, integer, atom) :: tuple
  defp streamer(buffer, 0, active_id, _) do
    {buffer, 0, active_id}
  end

  @spec streamer(binary, integer, integer, atom) :: tuple
  defp streamer(<<>>, length, active_id, _) do
    {<<>>, length, active_id}
  end

  @spec streamer(binary, integer, integer, atom) :: tuple
  defp streamer(buffer, length, active_id, r_info) when byte_size(buffer) < length do
    stream_me(:stream, buffer, active_id, r_info)
    left_length = length - byte_size(buffer)
    streamer(<<>>, left_length, active_id, r_info)
  end

  @spec streamer(binary, integer, integer, atom) :: tuple
  defp streamer(buffer, length, active_id, r_info) when byte_size(buffer) >= length do
    <<buffer::size(length)-bytes, rest::binary>> = buffer
    stream_me(:end, buffer, active_id, r_info)
    streamer(rest, 0, nil, r_info) # NOTE: changed active_id from 0 to nil
  end


  @spec stream_me(atom, binary, integer, atom) :: atom
  defp stream_me(action, buffer, stream_id, {ring_key, shard, conn}) do
    pid = FastGlobal.get(ring_key)[shard][conn][stream_id]
    GenServer.cast(pid, {action, stream_id, buffer}) # NOTE: changed from send to cast, to not break the receiver and decrease the latency.
  end

end
