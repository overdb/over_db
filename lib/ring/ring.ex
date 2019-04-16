defmodule OverDB.Ring do

  @min_token -9223372036854775808
  @max_token 9223372036854775807


  def gen_functions(ranges, module, otp_app) do
    Module.create(module, [gen_preparer(otp_app), gen_lookup_replicas() | gen_lookup_primary(ranges, otp_app)], Macro.Env.location(__ENV__))
    ranges
  end

  def gen_preparer(otp_app) do
    preparer = :"#{otp_app}_preparer"
    quote do
      def preparer?() do
        unquote(preparer)
      end
    end
  end

  def gen_lookup_primary(ranges, otp_app) do
    for { range , host_id} <- ranges do
      guard?(range, host_id, otp_app)
    end
  end

  # TODO: add rf as an addtional input to the bellow functions. or
  # TODO: hardcode the replicas(up to custom rf or all the possible) in second round ring.. this will provide instant access to the replicas directly from any given token.
  def gen_lookup_replicas() do
    quote do
      def lookup_replicas_atom(token) do
        {b, primary} = lookup_primary_atom(token)
        lookup_replicas(b, [primary])
      end
      def lookup_replicas(token) do
        {b, primary} = lookup_primary(token)
        lookup_replicas(b, [primary])
      end
      def lookup_replicas(nil, primary_replica) do
        primary_replica
      end
      def lookup_replicas(start, replicas) do
        {next, host_id_rf_2} = lookup_primary(start)
        # min_token, rf2
        lookup_replicas(next, [host_id_rf_2 | replicas], start)
      end
      def lookup_replicas(b, replicas, start) when b == start  do
        :lists.reverse(replicas)  # we should break from here.
        # this walk the whole ring and found all the possible replicas for simpleStrategy topology only..
        # this should be min length of 2.
        # the header will be the primary replica and the tail will contain the possible seconadry replicas :)
      end
      def lookup_replicas(b, replicas, start) do
        {next, host_id_rf_x} = lookup_primary(b) # this should run .. # host_id_rf_x may be the primary_replica if next == start
        # but we dont care as we are not going to add it to replicas list...
        # add host_id_rf_x to replicas if it does not already exist
        # then we run lookup_replicas again
        replicas =
          if Enum.any?(replicas, fn(replica) -> replica == host_id_rf_x end) do
            replicas
          else
            [host_id_rf_x | replicas]
          end
        lookup_replicas(next, replicas, start)
      end
    end
  end
  def guard?({@min_token, @max_token}, {{a, b, c, d}, nr, ig} = host_id, otp_app) do
    ring_key = :"#{otp_app}_#{a}.#{b}.#{c}.#{d}"
    quote do
      def lookup_primary(token) when is_integer(token) and token >= unquote(@min_token) and token <= unquote(@max_token) do
        {nil, unquote(Macro.escape(host_id))}
      end
      def lookup_primary_atom(token) when is_integer(token) and token >= unquote(@min_token) and token <= unquote(@max_token) do
        {nil, {unquote(ring_key), unquote(nr), unquote(ig)}}
      end
    end
  end

  def guard?({@min_token, b}, {{a, b, c, d}, nr, ig} = host_id, otp_app) do
    ring_key = :"#{otp_app}_#{a}.#{b}.#{c}.#{d}"
    quote do
      def lookup_primary(token) when is_integer(token) and token >= unquote(@min_token) and token <= unquote(b) do
        {unquote(b)+1, unquote(Macro.escape(host_id))}
      end
      def lookup_primary_atom(token) when is_integer(token) and token >= unquote(@min_token) and token <= unquote(b) do
        {unquote(b)+1, {unquote(ring_key), unquote(nr), unquote(ig)}}
      end
    end
  end

  def guard?({a, @max_token}, {{a, b, c, d}, nr, ig} = host_id, otp_app) do
    ring_key = :"#{otp_app}_#{a}.#{b}.#{c}.#{d}"
    quote do
      def lookup_primary(token) when is_integer(token) and token > unquote(a) and token <= unquote(@max_token) do
        {unquote(@min_token), unquote(Macro.escape(host_id))}
      end
      def lookup_primary_atom(token) when is_integer(token) and token > unquote(a) and token <= unquote(@max_token) do
        {unquote(@min_token), {unquote(ring_key), unquote(nr), unquote(ig)}}
      end
    end
  end

  def guard?({a, b}, {{a, b, c, d}, nr, ig} = host_id, otp_app) do
    ring_key = :"#{otp_app}_#{a}.#{b}.#{c}.#{d}"
    quote do
      def lookup_primary(token) when is_integer(token) and token > unquote(a) and token <= unquote(b) do
        {unquote(b)+1, unquote(Macro.escape(host_id))}
      end
      def lookup_primary_atom(token) when is_integer(token) and token > unquote(a) and token <= unquote(b) do
        {unquote(b)+1, {unquote(ring_key), unquote(nr), unquote(ig)}}
      end
    end
  end



end
