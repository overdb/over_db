defmodule OverDB.Connection do

  alias OverDB.Protocol.V4.Frames.{Frame, Requests.Startup, Requests.Options, Responses.Supported, Responses.Ready}

  @sync_opts [packet: :raw, mode: :binary, active: false, buffer: 10000000000] # TODO: remove the buffer tcp_window
  @async_active_true [packet: :raw, mode: :binary, active: true]
  @async_active_n [packet: :raw, mode: :binary, active: 32767]
  @async_active_once [packet: :raw, mode: :binary, active: :once]
  @timeout 5_000


  defstruct [:socket, :options, :address, :port, :strategy]

  @type t :: %__MODULE__{socket: port, options: map , address: list, port: integer, strategy: atom}


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
        |> Startup.new() |> push(conn.socket)
        recv(conn.socket, options[:strategy]) |> Frame.decode() |>  Ready.decode() |> check(conn)
      error -> error
    end
  end

  defp check(:ready, conn) do
    conn
  end

  @spec connect(map) :: t
  defp connect(%{address: address, port: port} =  options) do
    strategy = Map.get(options, :strategy, :async_active_true)
    timeout = Map.get(options, :timeout, @timeout)
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
      {:ok, socket} -> create(socket, address, port, strategy, get_cql_opts(socket, strategy))
      _ -> conn_status
    end
  end

  @spec get_cql_opts(port, atom) :: map
  def get_cql_opts(socket, :sync) do
    Options.new |> push(socket)
    recv(socket, :sync) |> Frame.decode() |> Supported.decode()
  end


  @spec get_cql_opts(port, atom) :: map
  def get_cql_opts(socket, _) do
    Options.new |> push(socket)
    receive do
      {:tcp, _socket, buffer} ->
         Frame.decode(buffer) |> Supported.decode()
    end
  end

  @spec push(list | binary, port) :: :ok
  def push(payload, socket) do
    :gen_tcp.send(socket, payload)
  end

  def recv(socket, :sync) do
    case :gen_tcp.recv(socket, 9) do
      {:ok, header} ->
        body_length = Frame.length(header)
        if body_length != 0 do
          case :gen_tcp.recv(socket, body_length) do
            {:ok, body} -> header <> body
            _ -> :error
          end
        else
          header
        end
      _ -> :error
    end
  end

  def recv(_socket, _) do
    receive do
      {:tcp, _socket, buffer} ->
         buffer
      _ -> :error
    end
  end

  @spec create(port, list, integer, atom, map) :: t
  defp create(socket, address, port, strategy, opts) do
    %__MODULE__{socket: socket, address: address, port: port, strategy: strategy, options: opts}
  end


  @spec connect_by_shard(String.t | integer, map, list) :: t
  defp connect_by_shard(shard, opts, acc) do
    shard = to_string(shard)
    {conn, sockets} = loop(shard, acc, opts)
    Enum.each(sockets, fn(x) -> :gen_tcp.close(x) end)
    conn
  end

  @spec loop(String.t, list, map) :: tuple
  defp loop(shard, acc, opts) do
    case loop(shard, opts) do
      {_, _} = error -> {error, acc}
      %__MODULE__{} = conn ->
        {conn, acc}
      x when is_port(x) -> loop(shard, [x | acc], opts)
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
    data_centers = Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__]
    for {_, nodes} <- data_centers do
      for node <- nodes do
        start(%{address: elem(node, 0), port: elem(node, 1), shard: shard, strategy: :sync})
      end
    end |> List.flatten
  end

end
