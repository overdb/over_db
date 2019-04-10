defmodule OverDB.Engine.Preparer do

  use GenServer
  alias OverDB.Protocol
  alias OverDB.Protocol.V4.Frames.Responses.Result.Prepared

  # TODO: this module have to be redone, as it has potintial bug in heavy load.

  def start_link(args) do
    name = :"#{args[:otp_app]}_preparer"
    GenServer.start_link(__MODULE__, {Map.new, []}, name: name)
  end

  def init(state) do
    {:ok, state}
  end

  def handle_call({:register, p_name}, _, {map, pids}) do
    {:reply, :ok, {map,[p_name | pids]}}
  end
  # # TODO: should check if the prepare cql already exist in the map_state to
  # prevent mixing the same prepare request..
  # solution : generating a random ref alongside the cql for each new request.
  def handle_cast({:prepare, cql, prepare_request}, {map, [first | rest] = pids}) do
    if is_nil(FastGlobal.get(cql)) do
      ref = :rand.uniform(99999999)
      map = Map.put(map, {cql, ref}, {length(pids), <<>>})
      GenStage.cast(first, {:prepare, self(), {cql, ref}, prepare_request, true})
      for pid <- rest do
        GenServer.cast(pid, {:prepare, self(), {cql, ref}, prepare_request, false})
      end
      {:noreply, {map, pids}}
    else
      {:noreply, {map, pids}}
    end
  end

  def handle_cast({:full, {cql, _} = cql_ref, buffer}, {map,pids}) do
    map =
      case Map.get(map, cql_ref) do # # TODO: there is a bug on heavy load.
        {0, <<>>} ->
          case Protocol.decode_frame(buffer, %{}) do
            %Prepared{id: _} = prepared? ->
              FastGlobal.put(cql, prepared?)
              Map.drop(map, [cql_ref])
            _ ->
              Map.drop(map, [cql_ref])
          end
        {counter, <<>>} ->
          case Protocol.decode_frame(buffer, %{}) do
            %Prepared{} = prepared? ->
              Map.put(map, cql_ref, {counter-1, prepared?})
            _ ->
              Map.drop(map, [cql_ref])
          end
        nil ->
          map
      end
      {:noreply, {map, pids}}
  end

  def handle_cast({:send?, {cql, _} = cql_ref, :ok}, {map, pids}) do
    map =
      case Map.get(map, cql_ref) do
        {1, %Prepared{id: _} = prepared?} ->
          FastGlobal.put(cql, prepared?)
          Map.drop(map, [cql_ref])
        {counter, value} ->
          Map.put(map, cql_ref, {counter-1, value})
        nil ->
          map
      end
    {:noreply, {map, pids}}
  end

  def handle_cast({:send?, cql_ref, _err}, {map, pids}) do
    {:noreply, {Map.drop(map,[cql_ref]), pids}}
  end

  def handle_cast({:start, cql_ref, buffer}, {map, pids}) do
    map =
      case Map.get(map,cql_ref) do
        {counter, _} ->
          Map.put(map, cql_ref, {counter, buffer})
        nil -> map
      end
    {:noreply, {map, pids}}
  end

  def handle_cast({:stream, cql_ref, buffer}, {map, pids}) do
    map =
      case Map.get(map,cql_ref) do
        {counter, old_buffer} ->
          Map.put(map, cql_ref, {counter, old_buffer <> buffer})
        nil -> map
      end
    {:noreply, {map, pids}}
  end

  def handle_cast({:end, {cql, _} = cql_ref, buffer}, {map, pids}) do
    map =
      case Map.get(map,cql_ref) do
        {counter, old_buffer} ->
          case Protocol.decode_frame(old_buffer <> buffer, %{}) do
            %Prepared{} = prepared? when counter == 0 ->
              FastGlobal.put(cql, prepared?)
              Map.drop(map, [cql_ref])
            %Prepared{} = prepared? when counter != 0 ->
              Map.put(map, cql_ref, {counter, prepared?})
            _ ->
              Map.drop(map, [cql_ref])
          end
        nil -> map
      end
    {:noreply, {map, pids}}
  end


end
