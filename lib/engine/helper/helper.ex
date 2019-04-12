defmodule OverDB.Engine.Helper do


  @spec tell_workers(list, atom) :: nil
  def tell_workers([{pid, ref, send_cast?} | ids], msg) do
    send_cast?(send_cast?, pid, msg, ref)
    tell_workers(ids, msg)
  end

  @spec tell_workers([], atom) :: nil
  def tell_workers([], _msg) do
    nil
  end

  @spec tell_workers(list, atom, binary | tuple) :: nil
  def tell_workers([{pid, ref, send_cast?} | ids], msg, content) do
    send_cast?(send_cast?, pid, msg, ref, content)
    tell_workers(ids, msg, content)
  end

  @spec tell_workers([], atom, binary | tuple) :: nil
  def tell_workers([], _, _) do
    nil
  end

  @spec tell_workers(list, atom, binary | tuple) :: nil
  def tell_workers([{pid, ref, send_cast?, boolean} | ids], msg, content) do
    if boolean do
      send_cast?(send_cast?, pid, msg, ref, content)
      tell_workers(ids, msg, content)
    else
      nil
    end
  end

  @spec send_cast?(integer, pid, atom, term, binary | tuple) :: atom
  defp send_cast?(0, pid, msg, ref, content) do
    send(pid, {msg, ref, content})
  end

  @spec send_cast?(integer, pid, atom, term, binary | tuple) :: atom
  defp send_cast?(1, pid, msg, ref, content) do
    GenStage.cast(pid, {msg, ref, content})
  end

  @spec send_cast?(integer, pid, atom, term) :: atom
  defp send_cast?(0, pid, msg, ref) do
    send(pid, {msg, ref})
  end

  @spec send_cast?(integer, pid, atom, term) :: atom
  defp send_cast?(1, pid, msg, ref) do
    GenStage.cast(pid, {msg, ref})
  end

  @spec gen_stream_ids(integer) :: list
  def gen_stream_ids(reporter_count?) when reporter_count? <= 32768 do
    reporters_ranges = split_stream_ids(32768, reporter_count?)
    gen_stream_ids(:start,reporters_ranges)
  end

  @spec gen_stream_ids(term) :: RuntimeError
  def gen_stream_ids(_) do
    raise "As there can only be 32768 different simultaneous streams (concurrent reporters per socket_conn)"
  end

  @spec gen_stream_ids(atom,list) :: list
  defp gen_stream_ids(:start,[h | t]) do
    head = {1, 1..h}
    gen_stream_ids(t, h, [head])
  end

  @spec gen_stream_ids(list, integer, list) :: list
  defp gen_stream_ids([h | t], start, [{num, _} | _t] = acc) do
    gen_stream_ids(t ,h+start,[{num+1, (start+1..h+start)} | acc])
  end

  @spec gen_stream_ids([], integer, list) :: list
  defp gen_stream_ids([], _, acc) do
    :lists.reverse(acc)
  end


  @spec gen_stream_ids(integer, integer, list) :: list
  defp split_stream_ids(range_end, reporter_count?) do
    range_count = trunc(range_end / reporter_count?)
    last_reporter = (range_end)-(range_count*(reporter_count?-1))
    for _ <- 1..reporter_count? do
      range_count
    end
    |> split_stream_ids(last_reporter-range_count, [])
  end


  defp split_stream_ids(list, 0, acc) do
    acc ++ list
  end


  defp split_stream_ids([h | t], remain, acc) do
    split_stream_ids(t, remain-1, [h+1| acc])
  end


  def register_me(0,1,1, name, otp_app) do
    :ok = GenStage.call(:"#{otp_app}_preparer",{:register, name})
  end

  def register_me(_,_,_, _, _) do
    nil
  end


  def subscribe_to(subscribe_to?, partition, logged, unlogged, counter) do
    subscribe_to?([], :logged, logged, subscribe_to?, partition)
    |> subscribe_to?(:unlogged, unlogged, subscribe_to?, partition)
    |> subscribe_to?(:counter, counter, subscribe_to?, partition)
  end

  def subscribe_to?(acc, _, nil, _, _) do
    acc
  end

  def subscribe_to?(acc, b_type, b_num, subscribe_to?, partition) do
    batchers =
      for num <- 1..b_num do
        {:"#{subscribe_to?}_#{b_type?(b_type)}#{num}", partition: partition}
      end
    acc ++ batchers
  end



  @spec b_type?(atom) :: atom
  def b_type?(:logged) do
    :l
  end

  @spec b_type?(atom) :: atom
  def b_type?(:unlogged) do
    :u
  end

  @spec b_type?(atom) :: atom
  def b_type?(:counter) do
    :c
  end



end
