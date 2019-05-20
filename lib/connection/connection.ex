defmodule OverDB.Connection do

  alias OverDB.Protocol.V4.Frames.{Frame, Requests.Startup, Requests.Options}
  alias OverDB.Protocol
  @sync_opts [packet: :raw, mode: :binary, active: false, buffer: 10000000000] # TODO: remove the buffer tcp_window
  @async_active_true [packet: :raw, mode: :binary, active: true]
  @async_active_n [packet: :raw, mode: :binary, active: 32767]
  @async_active_once [packet: :raw, mode: :binary, active: :once]
  @timeout 10_000


  defstruct [:socket, :options, :address, :port, :strategy, :data_center]

  @type t :: %__MODULE__{socket: port, options: map , address: list, port: integer, strategy: atom, data_center: atom}


  @spec start(map) :: t
  def start(options) do
    conn = connect_by_shard(options[:shard], options, [])
    case conn do
      %__MODULE__{} ->
        case Map.get(options, :compression) do
          nil ->
            %{"CQL_VERSION" => hd(conn.options[:CQL_VERSION])}
          :lz4 -> %{"CQL_VERSION" => hd(conn.options[:CQL_VERSION]), "COMPRESSION" => "lz4"}
          :snappy -> %{"CQL_VERSION" => hd(conn.options[:CQL_VERSION]), "COMPRESSION" => "snappy"}
        end
        |> Startup.new() |> push(conn.socket) |> recv(options[:strategy])
        |> Protocol.decode_frame() |> ensure_ready(conn)
      error -> error
    end
  end

  @spec connect(map) :: t
  defp connect(%{address: address, port: port} =  options) do
    strategy = Map.get(options, :strategy, :async_active_true)
    timeout = Map.get(options, :timeout, @timeout)
    Process.put(:timeout, timeout)
    conn_status =
      case strategy do
        :async_active_once ->
          :gen_tcp.connect(address, port, @async_active_once, timeout)
        :async_active_n ->
          :gen_tcp.connect(address, port, @async_active_n, timeout)
        :async_active_true ->
          :gen_tcp.connect(address, port, @async_active_true, timeout)
        :sync ->
          :gen_tcp.connect(address, port, @sync_opts, timeout)
      end
    case conn_status do
      {:ok, socket} ->
        case get_cql_opts(socket, strategy) do
          {:error, _} = err? ->
            :gen_tcp.close(socket)
            err?
          # TODO: work on supported struct instead of map in the protocol responses.
          cql_opts ->
            create(socket, address, port, strategy, cql_opts)
        end
      _ -> conn_status
    end
  end

  @spec get_cql_opts(port, atom) :: map
  def get_cql_opts(socket, strategy) do
    Options.new |> push(socket)
    |> recv(strategy) |> Protocol.decode_frame() |> ensure_supported()
  end

  @spec push(list | binary, port) :: :ok
  def push(payload, socket) do
    case :gen_tcp.send(socket, payload) do
      :ok ->
        socket
      err? -> err?
    end
  end

  @spec sync_recv(port) :: binary | tuple
  def sync_recv(socket) do
    recv(socket, :sync)
  end

  @spec recv(port, atom) :: binary | tuple
  defp recv(socket, :sync) when is_port(socket) do
    case :gen_tcp.recv(socket, 9, @timeout) do
      {:ok, header} ->
        body_length = Frame.length(header)
        if body_length != 0 do
          case :gen_tcp.recv(socket, body_length, @timeout) do
            {:ok, body} ->
              header <> body
            err? -> err?
          end
        else
          header
        end
      err? ->
        err?
    end
  end

  @spec recv(port, atom) :: binary | tuple
  defp recv(socket, _) when is_port(socket) do
    receive do
      {:tcp, _socket, buffer} ->
         buffer
      err? -> err?
    end
  end

  @spec recv(term, term) :: term
  defp recv(err?, _) do
    err?
  end

  @spec create(port, list, integer, atom, map) :: t
  defp create(socket, address, port, strategy, opts) do
    %__MODULE__{socket: socket, address: address, port: port, strategy: strategy, options: opts}
  end


  @spec connect_by_shard(String.t | integer, map, list) :: t
  defp connect_by_shard(int_shard, opts, acc) do
    shard = to_string(int_shard)
    case connect(opts) do
      %__MODULE__{options: %{SCYLLA_NR_SHARDS: [nr]}, socket: socket} ->
        case String.to_integer(nr) do
          max_nr when int_shard >= max_nr or int_shard < 0 ->
            :gen_tcp.close(socket)
            {:error, max_nr-1}
          _ ->
            :gen_tcp.close(socket)
            {conn_err?, sockets} = loop(shard, acc, opts)
            Enum.each(sockets, fn(port) -> :gen_tcp.close(port) end)
            conn_err?
        end
      err? ->
        err?
    end

  end

  @spec loop(String.t, list, map) :: tuple
  defp loop(shard, acc, opts) do
    case loop(shard, opts) do
      {_, _} = error -> {error, acc}
      %__MODULE__{} = conn ->
        {conn, acc}
      port when is_port(port) -> loop(shard, [port | acc], opts)
    end
  end

  @spec loop(String.t, map) :: t | port
  defp loop(shard, opts) do
      case connect(opts) do
        %__MODULE__{options: %{SCYLLA_SHARD: [current_shard]}, socket: socket} when current_shard != shard ->
          socket
        %__MODULE__{} = conn ->
          conn
        error -> error
      end
  end

  # this used only to fetch the ring(tokens) from all the nodes.
  @spec start_all(integer) :: list
  def start_all(otp_app, shard \\ 0) do
    Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__]
    |> start_all_conns(shard)
  end

  @spec start_all_conns(list, integer) :: tuple
  def start_all_conns(data_centers, shard) when is_integer(shard) and is_list(data_centers) do
    start_all_conns(data_centers,shard, {[], []})
  end

  @spec start_all_conns(list, integer, list) :: tuple
  defp start_all_conns([{dc, nodes}|dcs], shard, acc) do
    acc = start_dc_conns(dc,nodes, shard, acc)
    start_all_conns(dcs, shard, acc)
  end

  @spec start_all_conns(list, integer, list) :: tuple
  defp start_all_conns([], _, acc) do
    acc
  end

  @spec start_dc_conns(atom, list, integer, tuple) :: tuple
  defp start_dc_conns(dc, [{address, port}| nodes], shard, {dead, active}) do
    acc =
      case  start(%{address: address, port: port, shard: shard, strategy: :sync}) do
        %__MODULE__{} = conn ->
          {dead, [%{conn | data_center: dc} | active]}
        err? ->
          d = {err?, {dc, address, port}}
          {[d | dead], active}
      end
    start_dc_conns(dc, nodes, shard, acc)
  end

  @spec start_dc_conns(atom, list, integer, tuple) :: tuple
  defp start_dc_conns(_, _, _, acc) do
    acc
  end
  # ensure functions

  @spec ensure_ready(atom, t) :: t
  defp ensure_ready(:ready, conn) do
    conn
  end

  @spec ensure_ready(term, t) :: tuple
  defp ensure_ready(err?, _) do
    {:error, err?}
  end

  @spec ensure_supported(map) :: map
  defp ensure_supported(%{CQL_VERSION: _} = response) do
    response
  end

  @spec ensure_supported(term) :: term
  defp ensure_supported(err?) do
    {:error, err?}
  end

end
