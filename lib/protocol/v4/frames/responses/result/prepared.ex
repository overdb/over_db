defmodule OverDB.Protocol.V4.Frames.Responses.Result.Prepared do
 @moduledoc """
   Prepared

   The result to a PREPARE message. The body of a Prepared result is:
     <id><metadata><result_metadata>
   where:
     - <id> is [short bytes] representing the prepared query ID.
     - <metadata> is composed of:
         <flags><columns_count><pk_count>[<pk_index_1>...<pk_index_n>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
       where:
         - <flags> is an [int]. The bits of <flags> provides information on the
           formatting of the remaining information. A flag is set if the bit
           corresponding to its `mask` is set. Supported masks and their flags
           are:
             0x0001    Global_tables_spec: if set, only one table spec (keyspace
                       and table name) is provided as <global_table_spec>. If not
                       set, <global_table_spec> is not present.
         - <columns_count> is an [int] representing the number of bind markers
           in the prepared statement.  It defines the number of <col_spec_i>
           elements.
         - <pk_count> is an [int] representing the number of <pk_index_i>
           elements to follow. If this value is zero, at least one of the
           partition key columns in the table that the statement acts on
           did not have a corresponding bind marker (or the bind marker
           was wrapped in a function call).
         - <pk_index_i> is a short that represents the index of the bind marker
           that corresponds to the partition key column in position i.
           For example, a <pk_index> sequence of [2, 0, 1] indicates that the
           table has three partition key columns; the full partition key
           can be constructed by creating a composite of the values for
           the bind markers at index 2, at index 0, and at index 1.
           This allows implementations with token-aware routing to correctly
           construct the partition key without needing to inspect table
           metadata.
         - <global_table_spec> is present if the Global_tables_spec is set in
           <flags>. If present, it is composed of two [string]s. The first
           [string] is the name of the keyspace that the statement acts on.
           The second [string] is the name of the table that the columns
           represented by the bind markers belong to.
         - <col_spec_i> specifies the bind markers in the prepared statement.
           There are <column_count> such column specifications, each with the
           following format:
             (<ksname><tablename>)?<name><type>
           The initial <ksname> and <tablename> are two [string] that are only
           present if the Global_tables_spec flag is not set. The <name> field
           is a [string] that holds the name of the bind marker (if named),
           or the name of the column, field, or expression that the bind marker
           corresponds to (if the bind marker is "anonymous").  The <type>
           field is an [option] that represents the expected type of values for
           the bind marker.  See the Rows documentation (section 4.2.5.2) for
           full details on the <type> field.

     - <result_metadata> is defined exactly the same as <metadata> in the Rows
       documentation (section 4.2.5.2).  This describes the metadata for the
       result set that will be returned when this prepared statement is executed.
       Note that <result_metadata> may be empty (have the No_metadata flag and
       0 columns, See section 4.2.5.2) and will be for any query that is not a
       Select. In fact, there is never a guarantee that this will be non-empty, so
       implementations should protect themselves accordingly. This result metadata
       is an optimization that allows implementations to later execute the
       prepared statement without requesting the metadata (see the Skip_metadata
       flag in EXECUTE).  Clients can safely discard this metadata if they do not
       want to take advantage of that optimization.

   Note that the prepared query ID returned is global to the node on which the query
   has been prepared. It can be used on any connection to that node
   until the node is restarted (after which the query must be reprepared).

 """

 defstruct [:id, :metadata, :metadata_length, :result_metadata, :result_metadata_length, :pk_indices, :values]

 @type t :: %__MODULE__{id: binary, metadata: list, metadata_length: integer, result_metadata: list, result_metadata_length: integer, pk_indices: list, values: list | map}


 @spec create(binary, list, integer, list | nil, integer, list, list | map) :: t
 def create(id, metadata, metadata_length, result_metadata, result_metadata_length, pk_indices, values \\ []) do
   %__MODULE__{id: id, metadata: metadata, metadata_length: metadata_length, result_metadata: result_metadata, result_metadata_length: result_metadata_length, pk_indices: pk_indices, values: values}
 end











end
