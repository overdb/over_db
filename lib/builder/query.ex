defmodule OverDB.Builder.Query do
  alias OverDB.Builder

  @limit_default 500
  @list_default []

  defmacro prepared(modules, prepared \\ @list_default) do
    Builder.build(:prepared, modules, prepared)
  end

  defmacro prepare(modules, prepare \\ false) do
    Builder.build(:prepare, modules, prepare)
  end

  defmacro insert(modules, insert \\ @list_default) do
    Builder.build(:insert, modules, insert)
  end

  defmacro select(modules, select \\ @list_default) do
    Builder.build(:select, modules, select)
  end

  defmacro cql(modules, cql) do
    Builder.build(:cql, modules, cql)
  end

  # TODO: not ready yet (for internal use only)
  defmacro range(modules, range) do
    Builder.build(:range, modules, range)
  end

  defmacro count(modules, count \\ false) do
    Builder.build(:count, modules, count)
  end

  defmacro where(modules, where \\ nil) do
    Builder.build(:where, modules, where)
  end

  defmacro assign(modules, assign \\ nil) do
    Builder.build(:assign, modules, assign)
  end

  defmacro reference(modules, ref \\ :rand.uniform(9223372036854775807)) do
    Builder.build(:ref, modules, ref)
  end

  defmacro opts(modules, opts \\ Macro.escape(%{})) do
      Builder.build(:opts, modules, opts)
  end

  defmacro limit(modules, limit \\ @limit_default) do
    Builder.build(:limit, modules, limit)
  end

  defmacro order_by(modules, order_by \\ @list_default) do
    Builder.build(:order_by, modules, order_by)
  end

  defmacro allow_filtering(modules, allow_filtering \\ false) do
    Builder.build(:allow_filtering, modules, allow_filtering)
  end

  defmacro update(modules, update \\ @list_default) do
    Builder.build(:update, modules, update)
  end

  defmacro delete(modules, delete \\ @list_default) do
    Builder.build(:delete, modules, delete)
  end

  defmacro send_cast?(modules, send_cast? \\ :send) do
    Builder.build(:send_cast?, modules, send_cast?)
  end

  defmacro prepare?(modules, prepare? \\ false) do
    Builder.build(:prepare?, modules, prepare?)
  end

  defmacro stream(modules, stream \\ false) do
    Builder.build(:stream, modules, stream)
  end

  defmacro values(modules, values) do
    Builder.build(:values, modules, values)
  end

  defmacro pk(modules, pk) do
    Builder.build(:pk, modules, pk)
  end

  defmacro pk(modules, type) do
    Builder.build(:type, modules, type)
  end
end
