defmodule OverDB.Protocol do

  # TODO: replacing Map.put in decode functions(all exept decode_start) to %{map | key: value}
  @type responses :: Result.t | Error.t | Ready.t | Event.t | Supported.t

  alias OverDB.Protocol.V4.Frames.{Frame, Responses.Decoder, Responses.Result, Responses.Error, Responses.Event, Responses.Ready, Responses.Supported, Responses.Result.Prepared, Responses.Result.Ignore}

  @spec decode_stream(binary, map) :: tuple
  def decode_stream(buffer,  %{next: next, type: :stream} = query_state) do
    decode_stream(next, buffer, query_state)
  end

  @spec decode_stream(binary, map) :: map
  def decode_stream(buffer, %{old_buffer: old_buffer, type: type} = q_s) when type != :stream do
    q_s = Map.put(q_s, old_buffer, old_buffer <> buffer)
    Ignore.create(q_s, type)
  end

  @spec decode_stream(atom, binary, map) :: tuple
  def decode_stream(:start, buffer, query_state) do
    old_buffer = query_state[:old_buffer]
    current_buffer =
      if old_buffer do
        old_buffer <> buffer
      else
        buffer
      end
    case Decoder.streamed_result(current_buffer) do
      {:ok, columns_count, has_more_pages, false, buffer} ->
        decode_stream_start(buffer, current_buffer, columns_count, false, has_more_pages, query_state)
      {:ok, _columns_count, _, <<>>, buffer} ->
        query_state = Map.put(query_state, :next, :start)
        query_state = Map.put(query_state, :old_buffer, buffer)
        Ignore.create(query_state, :start)
      {:ok, columns_count, has_more_pages, paging_state, buffer} ->
        decode_stream_start(buffer, current_buffer, columns_count, paging_state, has_more_pages, query_state)
      {:short , buffer} ->
        query_state = Map.put(query_state, :next, :start)
        query_state = Map.put(query_state, :old_buffer, buffer)
        Ignore.create(query_state, :start)
    end
  end

  @spec decode_stream(atom, binary, map) :: tuple
  def decode_stream(:stream, buffer, %{result_metadata: columns, opts: opts, row_count: row_count, columns_count: columns_count, old_buffer: old_buffer} = query_state) do
    {rows_list, row_count, buffer} = Decoder.streamed_rows(row_count, columns_count, columns, old_buffer <> buffer, [], opts, opts[:function])
    query_state = Map.put(query_state, :next, next?(row_count))
    query_state = Map.put(query_state, :row_count, row_count)
    query_state = Map.put(query_state, :old_buffer, buffer)
    {rows_list, query_state} # rows_list may be a result of %Compute as well
  end

  def decode_stream_start(<<row_count::32-signed, buffer::binary>>, _, columns_count, paging_state, has_more_pages, %{result_metadata: columns, opts: opts}= query_state) do
    {rows_list, row_count, buffer} = Decoder.streamed_rows(row_count, columns_count, columns, buffer, [], opts, opts[:function])
    query_state = Map.put(query_state, :row_count, row_count)
    query_state = Map.put(query_state, :columns_count, columns_count)
    query_state = Map.put(query_state, :old_buffer, buffer)
    query_state = Map.put(query_state, :next, :stream)
    query_state = Map.put(query_state, :paging_state, paging_state)
    query_state = Map.put(query_state, :has_more_pages, has_more_pages)
    {rows_list, query_state} # rows_list may be a result of %Compute as well
  end

  def decode_stream_start(_, current_buffer, _column_count, _paging_state, _has_more_pages, query_state) do
    query_state = Map.put(query_state, :next, :start)
    query_state = Map.put(query_state, :old_buffer, current_buffer)
    Ignore.create(query_state, :start)
  end

  @spec next?(integer) :: atom
  def next?(0) do
    :ended
  end

  @spec next?(term) :: atom
  def next?(_) do
    :stream
  end

  @spec decode_full(binary, map) :: tuple
  def decode_full(<<_::32, 0x08, _::32, 0x0002::32-signed, _::binary>> = b, %{type: :stream} = q_s)do
    decode_start(b, q_s)
  end

  @spec decode_full(binary, map) :: responses
  def decode_full(buffer, %{opts: opts} = _query_state)do
    decode_frame(buffer, opts)
  end

  @spec decode_start(binary, map) :: tuple
  def decode_start(<<_header::9-bytes, body::binary>>, %{type: :stream} = query_state)do
    decode_stream(:start, body, query_state)
  end

  @spec decode_start(binary, map) :: map
  def decode_start(buffer, query_state)do
    Map.put(query_state, :old_buffer, buffer)
  end

  @spec decode_end(binary, map) :: responses
  def decode_end(buffer, %{type: :stream} = q_s) do
    decode_stream(buffer, q_s)
  end

  @spec decode_end(binary, map) :: responses
  def decode_end(buffer, %{old_buffer: old_buffer, type: type, opts: opts}) when type != :stream do
    decode_frame(old_buffer <> buffer, opts)
  end

  @spec decode_all(atom, binary, map) :: term
  def decode_all(:full, buffer, query_state) do
    decode_full(buffer, query_state)
  end

  @spec decode_all(atom, binary, map) :: term
  def decode_all(:start, buffer, query_state) do
    decode_start(buffer, query_state)
  end

  @spec decode_all(atom, binary, map) :: term
  def decode_all(:stream, buffer, query_state) do
    decode_stream(buffer, query_state)
  end

  @spec decode_all(atom, binary, map) :: term
  def decode_all(:end, buffer, query_state) do
    decode_end(buffer, query_state)
  end

  @spec decode_all(atom, binary, map) :: term
  def decode_all(:send?, status, _query_state) do
    {:send?, status}
  end

  @spec decode_response(port, map) :: responses
  def decode_response(socket, opts) do
    {:ok, <<_::5-bytes, len::32>> = header} = :gen_tcp.recv(socket, 9)
    {:ok, buffer} = :gen_tcp.recv(socket, len)
    decode_frame(header <> buffer, opts)
  end

  @spec decode_frame(binary, map) :: responses
  def decode_frame(buffer, opts) do
    flags = Map.get(opts, :flags, %{ignore: true})
    Frame.decode(buffer, flags) |> decode(opts)
  end

  @spec decode_response(port,Prepared.t, map) :: responses
  def decode_response(socket, prepared, opts) do
    buffer = :gen_tcp.recv(socket, 0)
    decode_frame(buffer, prepared, opts)
  end

  @spec decode_frame(binary, Prepared.t, map) :: responses
  defp decode_frame(buffer, prepared, opts) do
    flags = Map.get(opts, :flags, %{ignore: true})
    Frame.decode(buffer, flags) |> decode(prepared, opts)
  end


  @spec decode(Frame.t, map) :: Result.t
  defp decode(%Frame{opcode: :result} = frame, opts) do
    Result.decode(frame, opts)
  end

  @spec decode(Frame.t, map) :: Error.t
  defp decode(%Frame{opcode: :error} = frame, _opts) do
    Error.decode(frame)
  end

  @spec decode(Frame.t, map) :: Event.t
  defp decode(%Frame{opcode: :event} = frame, _) do
    Event.decode(frame)
  end

  @spec decode(Frame.t, map) :: Ready.t
  defp decode(%Frame{opcode: :ready} = frame, _) do
    Ready.decode(frame)
  end

  @spec decode(Frame.t, map) :: Supported.t
  defp decode(%Frame{opcode: :supported} = frame, _) do
    Supported.decode(frame)
  end

  @spec decode(Frame.t, Prepared.t, map) :: Result.t
  defp decode(%Frame{opcode: :result} = frame, prepared, opts) do
    Result.decode(frame, prepared, opts)
  end

  @spec decode(Frame.t, Prepared.t, map) :: Error.t
  defp decode(%Frame{opcode: :error} = frame, _prepared, _opts) do
    Error.decode(frame)
  end




end
