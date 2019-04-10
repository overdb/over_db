defmodule OverDB.Protocol.V4.Frames.Responses.Event do

  alias OverDB.Protocol.V4.Frames.Responses.Decoder
  alias OverDB.Protocol.V4.Frames.Frame

  defstruct [:event, :effect, :target, :options]

  @type t :: %__MODULE__{event: atom, effect: atom, target: tuple | String.t, options: String.t | tuple | list}


  def opcode() do
    0x0C
  end

  defguard is_table_or_type?(v) when v == "TABLE" or v == "TYPE"
  defguard is_fun_or_agg?(v) when v == "FUNCTION" or v == "AGGREGATE"

  @spec decode(Frame.t) :: t
  def decode(%Frame{opcode: :event, body: body}) do
    {event, rest} = Decoder.string(body)
    decode(:"#{event}", rest)
  end

  @spec decode(atom, binary) :: t
  def decode(:SCHEMA_CHANGE = event, rest) do
    {change_type, rest} = Decoder.string(rest)
    {target, rest} = Decoder.string(rest)
    options =
      case target do
        "KEYSPACE" ->
          {keyspace ,_rest} = Decoder.string(rest)
          keyspace
        target when is_table_or_type?(target) ->
          {keyspace, rest} = Decoder.string(rest)
          {object, <<>>} = Decoder.string(rest)
          {keyspace, object}
        target when is_fun_or_agg?(target) ->
          {keyspace, rest} = Decoder.string(rest)
          {fun_or_agg, rest} = Decoder.string(rest)
          {arg_type_list, <<>>} = Decoder.string_list(rest)
          {keyspace, fun_or_agg, arg_type_list}
      end
    create(event, :"#{change_type}", target, options)
  end


  @spec decode(atom, binary) :: t
  def decode(:STATUS_CHANGE = event, rest) do
    {effect, rest} = Decoder.string(rest)
    {address, port, <<>>} = Decoder.inet(rest)
    create(event, :"#{effect}", {address, port}, [])
  end

  @spec create(atom, atom, tuple, list | String.t) :: t
  def create(event, effect, target, options) do
    %__MODULE__{event: event, effect: effect, target: target, options: options}
  end


end
