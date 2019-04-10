defmodule OverDB.Builder.Executor do

  defmacro __using__(opts) do
    quote do
      alias OverDB.Protocol.V4.Frames.Requests.{Query, Execute, Prepare}
      alias OverDB.Protocol.V4.Frames.Responses.Result.Prepared
      alias OverDB.Ring.Shard
      alias OverDB.Builder.Cql
      alias OverDB.Builder.Executor.Execute
      @otp_app unquote(opts[:otp_app])
      @conns Application.get_env(:over_db, @otp_app)[:__CONNS_PER_SHARD__]
      @reporters Application.get_env(:over_db, @otp_app)[:__REPORTERS_PER_SHARD__]
      @logged Application.get_env(:over_db, @otp_app)[:__LOGGED_PER_SHARD__]
      @unlogged Application.get_env(:over_db, @otp_app)[:__UNLOGGED_PER_SHARD__]
      @counter Application.get_env(:over_db, @otp_app)[:__COUNTER_PER_SHARD__]
      @ring if unquote(opts[:ring]), do: unquote(opts[:ring]), else: Application.get_env(:over_db, @otp_app)[:__RING__]


      if @logged do
        def logged(query) when is_map(query) do
          Execute.in_batch(query,:rand.uniform(@conns),:l, :rand.uniform(@logged), @ring)
        end
      end

      if @unlogged do
        def unlogged(query) when is_map(query) do
          Execute.in_batch(query,:rand.uniform(@conns), :u, :rand.uniform(@unlogged), @ring)
        end
      end

      if @counter do
        def counter(query) when is_map(query) do
          Execute.in_batch(query,:rand.uniform(@conns), :c, :rand.uniform(@unlogged), @ring)
        end
      end

      if @reporters do
        def query(query) when is_map(query) do
          Execute.alone(query,:rand.uniform(@conns), @ring)
        end      
      end

    end
  end

end
