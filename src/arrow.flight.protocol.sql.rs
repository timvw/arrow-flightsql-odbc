///
/// Represents a metadata request. Used in the command member of FlightDescriptor
/// for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the metadata request.
///
/// The returned Arrow schema will be:
/// <
///  info_name: uint32 not null,
///  value: dense_union<
///              string_value: utf8,
///              bool_value: bool,
///              bigint_value: int64,
///              int32_bitmask: int32,
///              string_list: list<string_data: utf8>
///              int32_to_int32_list_map: map<key: int32, value: list<$data$: int32>>
/// >
/// where there is one row per requested piece of metadata information.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetSqlInfo {
    ///
    /// Values are modelled after ODBC's SQLGetInfo() function. This information is intended to provide
    /// Flight SQL clients with basic, SQL syntax and SQL functions related information.
    /// More information types can be added in future releases.
    /// E.g. more SQL syntax support types, scalar functions support, type conversion support etc.
    ///
    /// Note that the set of metadata may expand.
    ///
    /// Initially, Flight SQL will support the following information types:
    /// - Server Information - Range [0-500)
    /// - Syntax Information - Range [500-1000)
    /// Range [0-10,000) is reserved for defaults (see SqlInfo enum for default options).
    /// Custom options should start at 10,000.
    ///
    /// If omitted, then all metadata will be retrieved.
    /// Flight SQL Servers may choose to include additional metadata above and beyond the specified set, however they must
    /// at least return the specified set. IDs ranging from 0 to 10,000 (exclusive) are reserved for future use.
    /// If additional metadata is included, the metadata IDs should start from 10,000.
    #[prost(uint32, repeated, tag="1")]
    pub info: ::prost::alloc::vec::Vec<u32>,
}
///
/// Represents a request to retrieve information about data type supported on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned schema will be:
/// <
///   type_name: utf8 not null (The name of the data type, for example: VARCHAR, INTEGER, etc),
///   data_type: int32 not null (The SQL data type),
///   column_size: int32 (The maximum size supported by that column.
///                       In case of exact numeric types, this represents the maximum precision.
///                       In case of string types, this represents the character length.
///                       In case of datetime data types, this represents the length in characters of the string representation.
///                       NULL is returned for data types where column size is not applicable.),
///   literal_prefix: utf8 (Character or characters used to prefix a literal, NULL is returned for
///                         data types where a literal prefix is not applicable.),
///   literal_suffix: utf8 (Character or characters used to terminate a literal,
///                         NULL is returned for data types where a literal suffix is not applicable.),
///   create_params: list<utf8 not null>
///                        (A list of keywords corresponding to which parameters can be used when creating
///                         a column for that specific type.
///                         NULL is returned if there are no parameters for the data type definition.),
///   nullable: int32 not null (Shows if the data type accepts a NULL value. The possible values can be seen in the
///                             Nullable enum.),
///   case_sensitive: bool not null (Shows if a character data type is case-sensitive in collations and comparisons),
///   searchable: int32 not null (Shows how the data type is used in a WHERE clause. The possible values can be seen in the
///                               Searchable enum.),
///   unsigned_attribute: bool (Shows if the data type is unsigned. NULL is returned if the attribute is
///                             not applicable to the data type or the data type is not numeric.),
///   fixed_prec_scale: bool not null (Shows if the data type has predefined fixed precision and scale.),
///   auto_increment: bool (Shows if the data type is auto incremental. NULL is returned if the attribute
///                         is not applicable to the data type or the data type is not numeric.),
///   local_type_name: utf8 (Localized version of the data source-dependent name of the data type. NULL
///                          is returned if a localized name is not supported by the data source),
///   minimum_scale: int32 (The minimum scale of the data type on the data source.
///                         If a data type has a fixed scale, the MINIMUM_SCALE and MAXIMUM_SCALE
///                         columns both contain this value. NULL is returned if scale is not applicable.),
///   maximum_scale: int32 (The maximum scale of the data type on the data source.
///                         NULL is returned if scale is not applicable.),
///   sql_data_type: int32 not null (The value of the SQL DATA TYPE which has the same values
///                                  as data_type value. Except for interval and datetime, which
///                                  uses generic values. More info about those types can be
///                                  obtained through datetime_subcode. The possible values can be seen
///                                  in the XdbcDataType enum.),
///   datetime_subcode: int32 (Only used when the SQL DATA TYPE is interval or datetime. It contains
///                            its sub types. For type different from interval and datetime, this value
///                            is NULL. The possible values can be seen in the XdbcDatetimeSubcode enum.),
///   num_prec_radix: int32 (If the data type is an approximate numeric type, this column contains
///                          the value 2 to indicate that COLUMN_SIZE specifies a number of bits. For
///                          exact numeric types, this column contains the value 10 to indicate that
///                          column size specifies a number of decimal digits. Otherwise, this column is NULL.),
///   interval_precision: int32 (If the data type is an interval data type, then this column contains the value
///                              of the interval leading precision. Otherwise, this column is NULL. This fields
///                              is only relevant to be used by ODBC).
/// >
/// The returned data should be ordered by data_type and then by type_name.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetXdbcTypeInfo {
    ///
    /// Specifies the data type to search for the info.
    #[prost(int32, optional, tag="1")]
    pub data_type: ::core::option::Option<i32>,
}
///
/// Represents a request to retrieve the list of catalogs on a Flight SQL enabled backend.
/// The definition of a catalog depends on vendor/implementation. It is usually the database itself
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8 not null
/// >
/// The returned data should be ordered by catalog_name.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetCatalogs {
}
///
/// Represents a request to retrieve the list of database schemas on a Flight SQL enabled backend.
/// The definition of a database schema depends on vendor/implementation. It is usually a collection of tables.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8,
///  db_schema_name: utf8 not null
/// >
/// The returned data should be ordered by catalog_name, then db_schema_name.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetDbSchemas {
    ///
    /// Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    #[prost(string, optional, tag="2")]
    pub db_schema_filter_pattern: ::core::option::Option<::prost::alloc::string::String>,
}
///
/// Represents a request to retrieve the list of tables, and optionally their schemas, on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8,
///  db_schema_name: utf8,
///  table_name: utf8 not null,
///  table_type: utf8 not null,
///  \[optional\] table_schema: bytes not null (schema of the table as described in Schema.fbs::Schema,
///                                           it is serialized as an IPC message.)
/// >
/// Fields on table_schema may contain the following metadata:
///  - ARROW:FLIGHT:SQL:CATALOG_NAME      - Table's catalog name
///  - ARROW:FLIGHT:SQL:DB_SCHEMA_NAME    - Database schema name
///  - ARROW:FLIGHT:SQL:TABLE_NAME        - Table name
///  - ARROW:FLIGHT:SQL:TYPE_NAME         - The data source-specific name for the data type of the column.
///  - ARROW:FLIGHT:SQL:PRECISION         - Column precision/size
///  - ARROW:FLIGHT:SQL:SCALE             - Column scale/decimal digits if applicable
///  - ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT - "1" indicates if the column is auto incremented, "0" otherwise.
///  - ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE - "1" indicates if the column is case sensitive, "0" otherwise.
///  - ARROW:FLIGHT:SQL:IS_READ_ONLY      - "1" indicates if the column is read only, "0" otherwise.
///  - ARROW:FLIGHT:SQL:IS_SEARCHABLE     - "1" indicates if the column is searchable via WHERE clause, "0" otherwise.
/// The returned data should be ordered by catalog_name, db_schema_name, table_name, then table_type, followed by table_schema if requested.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetTables {
    ///
    /// Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, all schemas matching other filters are searched.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    #[prost(string, optional, tag="2")]
    pub db_schema_filter_pattern: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter pattern for tables to search for.
    /// When no table_name_filter_pattern is provided, all tables matching other filters are searched.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    #[prost(string, optional, tag="3")]
    pub table_name_filter_pattern: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter of table types which must match.
    /// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
    /// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
    #[prost(string, repeated, tag="4")]
    pub table_types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Specifies if the Arrow schema should be returned for found tables.
    #[prost(bool, tag="5")]
    pub include_schema: bool,
}
///
/// Represents a request to retrieve the list of table types on a Flight SQL enabled backend.
/// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
/// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  table_type: utf8 not null
/// >
/// The returned data should be ordered by table_type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetTableTypes {
}
///
/// Represents a request to retrieve the primary keys of a table on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8,
///  db_schema_name: utf8,
///  table_name: utf8 not null,
///  column_name: utf8 not null,
///  key_name: utf8,
///  key_sequence: int32 not null
/// >
/// The returned data should be ordered by catalog_name, db_schema_name, table_name, key_name, then key_sequence.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetPrimaryKeys {
    ///
    /// Specifies the catalog to search for the table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies the schema to search for the table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub db_schema: ::core::option::Option<::prost::alloc::string::String>,
    /// Specifies the table to get the primary keys for.
    #[prost(string, tag="3")]
    pub table: ::prost::alloc::string::String,
}
///
/// Represents a request to retrieve a description of the foreign key columns that reference the given table's
/// primary key columns (the foreign keys exported by a table) of a table on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  pk_catalog_name: utf8,
///  pk_db_schema_name: utf8,
///  pk_table_name: utf8 not null,
///  pk_column_name: utf8 not null,
///  fk_catalog_name: utf8,
///  fk_db_schema_name: utf8,
///  fk_table_name: utf8 not null,
///  fk_column_name: utf8 not null,
///  key_sequence: int32 not null,
///  fk_key_name: utf8,
///  pk_key_name: utf8,
///  update_rule: uint8 not null,
///  delete_rule: uint8 not null
/// >
/// The returned data should be ordered by fk_catalog_name, fk_db_schema_name, fk_table_name, fk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions declared on UpdateDeleteRules enum.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetExportedKeys {
    ///
    /// Specifies the catalog to search for the foreign key table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies the schema to search for the foreign key table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub db_schema: ::core::option::Option<::prost::alloc::string::String>,
    /// Specifies the foreign key table to get the foreign keys for.
    #[prost(string, tag="3")]
    pub table: ::prost::alloc::string::String,
}
///
/// Represents a request to retrieve the foreign keys of a table on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  pk_catalog_name: utf8,
///  pk_db_schema_name: utf8,
///  pk_table_name: utf8 not null,
///  pk_column_name: utf8 not null,
///  fk_catalog_name: utf8,
///  fk_db_schema_name: utf8,
///  fk_table_name: utf8 not null,
///  fk_column_name: utf8 not null,
///  key_sequence: int32 not null,
///  fk_key_name: utf8,
///  pk_key_name: utf8,
///  update_rule: uint8 not null,
///  delete_rule: uint8 not null
/// >
/// The returned data should be ordered by pk_catalog_name, pk_db_schema_name, pk_table_name, pk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions:
///    - 0 = CASCADE
///    - 1 = RESTRICT
///    - 2 = SET NULL
///    - 3 = NO ACTION
///    - 4 = SET DEFAULT
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetImportedKeys {
    ///
    /// Specifies the catalog to search for the primary key table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies the schema to search for the primary key table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub db_schema: ::core::option::Option<::prost::alloc::string::String>,
    /// Specifies the primary key table to get the foreign keys for.
    #[prost(string, tag="3")]
    pub table: ::prost::alloc::string::String,
}
///
/// Represents a request to retrieve a description of the foreign key columns in the given foreign key table that
/// reference the primary key or the columns representing a unique constraint of the parent table (could be the same
/// or a different table) on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  pk_catalog_name: utf8,
///  pk_db_schema_name: utf8,
///  pk_table_name: utf8 not null,
///  pk_column_name: utf8 not null,
///  fk_catalog_name: utf8,
///  fk_db_schema_name: utf8,
///  fk_table_name: utf8 not null,
///  fk_column_name: utf8 not null,
///  key_sequence: int32 not null,
///  fk_key_name: utf8,
///  pk_key_name: utf8,
///  update_rule: uint8 not null,
///  delete_rule: uint8 not null
/// >
/// The returned data should be ordered by pk_catalog_name, pk_db_schema_name, pk_table_name, pk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions:
///    - 0 = CASCADE
///    - 1 = RESTRICT
///    - 2 = SET NULL
///    - 3 = NO ACTION
///    - 4 = SET DEFAULT
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetCrossReference {
    ///*
    /// The catalog name where the parent table is.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub pk_catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The Schema name where the parent table is.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub pk_db_schema: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The parent table name. It cannot be null.
    #[prost(string, tag="3")]
    pub pk_table: ::prost::alloc::string::String,
    ///*
    /// The catalog name where the foreign table is.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="4")]
    pub fk_catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The schema name where the foreign table is.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="5")]
    pub fk_db_schema: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The foreign table name. It cannot be null.
    #[prost(string, tag="6")]
    pub fk_table: ::prost::alloc::string::String,
}
// Query Execution Action Messages

///
/// Request message for the "CreatePreparedStatement" action on a Flight SQL enabled backend.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCreatePreparedStatementRequest {
    /// The valid SQL string to create a prepared statement for.
    #[prost(string, tag="1")]
    pub query: ::prost::alloc::string::String,
    /// Create/execute the prepared statement as part of this transaction (if
    /// unset, executions of the prepared statement will be auto-committed).
    #[prost(bytes="vec", optional, tag="2")]
    pub transaction_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
///
/// An embedded message describing a Substrait plan to execute.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubstraitPlan {
    /// The serialized substrait.Plan to create a prepared statement for.
    /// XXX(ARROW-16902): this is bytes instead of an embedded message
    /// because Protobuf does not really support one DLL using Protobuf
    /// definitions from another DLL.
    #[prost(bytes="vec", tag="1")]
    pub plan: ::prost::alloc::vec::Vec<u8>,
    /// The Substrait release, e.g. "0.12.0". This information is not
    /// tracked in the plan itself, so this is the only way for consumers
    /// to potentially know if they can handle the plan.
    #[prost(string, tag="2")]
    pub version: ::prost::alloc::string::String,
}
///
/// Request message for the "CreatePreparedSubstraitPlan" action on a Flight SQL enabled backend.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCreatePreparedSubstraitPlanRequest {
    /// The serialized substrait.Plan to create a prepared statement for.
    #[prost(message, optional, tag="1")]
    pub plan: ::core::option::Option<SubstraitPlan>,
    /// Create/execute the prepared statement as part of this transaction (if
    /// unset, executions of the prepared statement will be auto-committed).
    #[prost(bytes="vec", optional, tag="2")]
    pub transaction_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
///
/// Wrap the result of a "CreatePreparedStatement" or "CreatePreparedSubstraitPlan" action.
///
/// The resultant PreparedStatement can be closed either:
/// - Manually, through the "ClosePreparedStatement" action;
/// - Automatically, by a server timeout.
///
/// The result should be wrapped in a google.protobuf.Any message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCreatePreparedStatementResult {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
    /// If a result set generating query was provided, dataset_schema contains the
    /// schema of the dataset as described in Schema.fbs::Schema, it is serialized as an IPC message.
    #[prost(bytes="vec", tag="2")]
    pub dataset_schema: ::prost::alloc::vec::Vec<u8>,
    /// If the query provided contained parameters, parameter_schema contains the
    /// schema of the expected parameters as described in Schema.fbs::Schema, it is serialized as an IPC message.
    #[prost(bytes="vec", tag="3")]
    pub parameter_schema: ::prost::alloc::vec::Vec<u8>,
}
///
/// Request message for the "ClosePreparedStatement" action on a Flight SQL enabled backend.
/// Closes server resources associated with the prepared statement handle.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionClosePreparedStatementRequest {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Request message for the "BeginTransaction" action.
/// Begins a transaction.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionBeginTransactionRequest {
}
///
/// Request message for the "BeginSavepoint" action.
/// Creates a savepoint within a transaction.
///
/// Only supported if FLIGHT_SQL_TRANSACTION is
/// FLIGHT_SQL_TRANSACTION_SUPPORT_SAVEPOINT.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionBeginSavepointRequest {
    /// The transaction to which a savepoint belongs.
    #[prost(bytes="vec", tag="1")]
    pub transaction_id: ::prost::alloc::vec::Vec<u8>,
    /// Name for the savepoint.
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
}
///
/// The result of a "BeginTransaction" action.
///
/// The transaction can be manipulated with the "EndTransaction" action, or
/// automatically via server timeout. If the transaction times out, then it is
/// automatically rolled back.
///
/// The result should be wrapped in a google.protobuf.Any message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionBeginTransactionResult {
    /// Opaque handle for the transaction on the server.
    #[prost(bytes="vec", tag="1")]
    pub transaction_id: ::prost::alloc::vec::Vec<u8>,
}
///
/// The result of a "BeginSavepoint" action.
///
/// The transaction can be manipulated with the "EndSavepoint" action.
/// If the associated transaction is committed, rolled back, or times
/// out, then the savepoint is also invalidated.
///
/// The result should be wrapped in a google.protobuf.Any message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionBeginSavepointResult {
    /// Opaque handle for the savepoint on the server.
    #[prost(bytes="vec", tag="1")]
    pub savepoint_id: ::prost::alloc::vec::Vec<u8>,
}
///
/// Request message for the "EndTransaction" action.
///
/// Commit (COMMIT) or rollback (ROLLBACK) the transaction.
///
/// If the action completes successfully, the transaction handle is
/// invalidated, as are all associated savepoints.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionEndTransactionRequest {
    /// Opaque handle for the transaction on the server.
    #[prost(bytes="vec", tag="1")]
    pub transaction_id: ::prost::alloc::vec::Vec<u8>,
    /// Whether to commit/rollback the given transaction.
    #[prost(enumeration="action_end_transaction_request::EndTransaction", tag="2")]
    pub action: i32,
}
/// Nested message and enum types in `ActionEndTransactionRequest`.
pub mod action_end_transaction_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum EndTransaction {
        Unspecified = 0,
        /// Commit the transaction.
        Commit = 1,
        /// Roll back the transaction.
        Rollback = 2,
    }
}
///
/// Request message for the "EndSavepoint" action.
///
/// Release (RELEASE) the savepoint or rollback (ROLLBACK) to the
/// savepoint.
///
/// Releasing a savepoint invalidates that savepoint.  Rolling back to
/// a savepoint does not invalidate the savepoint, but invalidates all
/// savepoints created after the current savepoint.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionEndSavepointRequest {
    /// Opaque handle for the savepoint on the server.
    #[prost(bytes="vec", tag="1")]
    pub savepoint_id: ::prost::alloc::vec::Vec<u8>,
    /// Whether to rollback/release the given savepoint.
    #[prost(enumeration="action_end_savepoint_request::EndSavepoint", tag="2")]
    pub action: i32,
}
/// Nested message and enum types in `ActionEndSavepointRequest`.
pub mod action_end_savepoint_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum EndSavepoint {
        Unspecified = 0,
        /// Release the savepoint.
        Release = 1,
        /// Roll back to a savepoint.
        Rollback = 2,
    }
}
// Query Execution Messages.

///
/// Represents a SQL query. Used in the command member of FlightDescriptor
/// for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///    Fields on this schema may contain the following metadata:
///    - ARROW:FLIGHT:SQL:CATALOG_NAME      - Table's catalog name
///    - ARROW:FLIGHT:SQL:DB_SCHEMA_NAME    - Database schema name
///    - ARROW:FLIGHT:SQL:TABLE_NAME        - Table name
///    - ARROW:FLIGHT:SQL:TYPE_NAME         - The data source-specific name for the data type of the column.
///    - ARROW:FLIGHT:SQL:PRECISION         - Column precision/size
///    - ARROW:FLIGHT:SQL:SCALE             - Column scale/decimal digits if applicable
///    - ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT - "1" indicates if the column is auto incremented, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE - "1" indicates if the column is case sensitive, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_READ_ONLY      - "1" indicates if the column is read only, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_SEARCHABLE     - "1" indicates if the column is searchable via WHERE clause, "0" otherwise.
///  - GetFlightInfo: execute the query.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandStatementQuery {
    /// The SQL syntax.
    #[prost(string, tag="1")]
    pub query: ::prost::alloc::string::String,
    /// Include the query as part of this transaction (if unset, the query is auto-committed).
    #[prost(bytes="vec", optional, tag="2")]
    pub transaction_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
///
/// Represents a Substrait plan. Used in the command member of FlightDescriptor
/// for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///    Fields on this schema may contain the following metadata:
///    - ARROW:FLIGHT:SQL:CATALOG_NAME      - Table's catalog name
///    - ARROW:FLIGHT:SQL:DB_SCHEMA_NAME    - Database schema name
///    - ARROW:FLIGHT:SQL:TABLE_NAME        - Table name
///    - ARROW:FLIGHT:SQL:TYPE_NAME         - The data source-specific name for the data type of the column.
///    - ARROW:FLIGHT:SQL:PRECISION         - Column precision/size
///    - ARROW:FLIGHT:SQL:SCALE             - Column scale/decimal digits if applicable
///    - ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT - "1" indicates if the column is auto incremented, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE - "1" indicates if the column is case sensitive, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_READ_ONLY      - "1" indicates if the column is read only, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_SEARCHABLE     - "1" indicates if the column is searchable via WHERE clause, "0" otherwise.
///  - GetFlightInfo: execute the query.
///  - DoPut: execute the query.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandStatementSubstraitPlan {
    /// A serialized substrait.Plan
    #[prost(message, optional, tag="1")]
    pub plan: ::core::option::Option<SubstraitPlan>,
    /// Include the query as part of this transaction (if unset, the query is auto-committed).
    #[prost(bytes="vec", optional, tag="2")]
    pub transaction_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
///*
/// Represents a ticket resulting from GetFlightInfo with a CommandStatementQuery.
/// This should be used only once and treated as an opaque value, that is, clients should not attempt to parse this.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TicketStatementQuery {
    /// Unique identifier for the instance of the statement to execute.
    #[prost(bytes="vec", tag="1")]
    pub statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Represents an instance of executing a prepared statement. Used in the command member of FlightDescriptor for
/// the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///    Fields on this schema may contain the following metadata:
///    - ARROW:FLIGHT:SQL:CATALOG_NAME      - Table's catalog name
///    - ARROW:FLIGHT:SQL:DB_SCHEMA_NAME    - Database schema name
///    - ARROW:FLIGHT:SQL:TABLE_NAME        - Table name
///    - ARROW:FLIGHT:SQL:TYPE_NAME         - The data source-specific name for the data type of the column.
///    - ARROW:FLIGHT:SQL:PRECISION         - Column precision/size
///    - ARROW:FLIGHT:SQL:SCALE             - Column scale/decimal digits if applicable
///    - ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT - "1" indicates if the column is auto incremented, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE - "1" indicates if the column is case sensitive, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_READ_ONLY      - "1" indicates if the column is read only, "0" otherwise.
///    - ARROW:FLIGHT:SQL:IS_SEARCHABLE     - "1" indicates if the column is searchable via WHERE clause, "0" otherwise.
///  - DoPut: bind parameter values. All of the bound parameter sets will be executed as a single atomic execution.
///  - GetFlightInfo: execute the prepared statement instance.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPreparedStatementQuery {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Represents a SQL update query. Used in the command member of FlightDescriptor
/// for the the RPC call DoPut to cause the server to execute the included SQL update.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandStatementUpdate {
    /// The SQL syntax.
    #[prost(string, tag="1")]
    pub query: ::prost::alloc::string::String,
    /// Include the query as part of this transaction (if unset, the query is auto-committed).
    #[prost(bytes="vec", optional, tag="2")]
    pub transaction_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
///
/// Represents a SQL update query. Used in the command member of FlightDescriptor
/// for the the RPC call DoPut to cause the server to execute the included
/// prepared statement handle as an update.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPreparedStatementUpdate {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Returned from the RPC call DoPut when a CommandStatementUpdate
/// CommandPreparedStatementUpdate was in the request, containing
/// results from the update.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoPutUpdateResult {
    /// The number of records updated. A return value of -1 represents
    /// an unknown updated record count.
    #[prost(int64, tag="1")]
    pub record_count: i64,
}
///
/// Request message for the "CancelQuery" action.
///
/// Explicitly cancel a running query.
///
/// This lets a single client explicitly cancel work, no matter how many clients
/// are involved/whether the query is distributed or not, given server support.
/// The transaction/statement is not rolled back; it is the application's job to
/// commit or rollback as appropriate. This only indicates the client no longer
/// wishes to read the remainder of the query results or continue submitting
/// data.
///
/// This command is idempotent.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCancelQueryRequest {
    /// The result of the GetFlightInfo RPC that initiated the query.
    /// XXX(ARROW-16902): this must be a serialized FlightInfo, but is
    /// rendered as bytes because Protobuf does not really support one
    /// DLL using Protobuf definitions from another DLL.
    #[prost(bytes="vec", tag="1")]
    pub info: ::prost::alloc::vec::Vec<u8>,
}
///
/// The result of cancelling a query.
///
/// The result should be wrapped in a google.protobuf.Any message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCancelQueryResult {
    #[prost(enumeration="action_cancel_query_result::CancelResult", tag="1")]
    pub result: i32,
}
/// Nested message and enum types in `ActionCancelQueryResult`.
pub mod action_cancel_query_result {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum CancelResult {
        /// The cancellation status is unknown. Servers should avoid using
        /// this value (send a NOT_FOUND error if the requested query is
        /// not known). Clients can retry the request.
        Unspecified = 0,
        /// The cancellation request is complete. Subsequent requests with
        /// the same payload may return CANCELLED or a NOT_FOUND error.
        Cancelled = 1,
        /// The cancellation request is in progress. The client may retry
        /// the cancellation request.
        Cancelling = 2,
        /// The query is not cancellable. The client should not retry the
        /// cancellation request.
        NotCancellable = 3,
    }
}
/// Options for CommandGetSqlInfo.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlInfo {
    // Server Information [0-500): Provides basic information about the Flight SQL Server.

    /// Retrieves a UTF-8 string with the name of the Flight SQL Server.
    FlightSqlServerName = 0,
    /// Retrieves a UTF-8 string with the native version of the Flight SQL Server.
    FlightSqlServerVersion = 1,
    /// Retrieves a UTF-8 string with the Arrow format version of the Flight SQL Server.
    FlightSqlServerArrowVersion = 2,
    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server is read only.
    ///
    /// Returns:
    /// - false: if read-write
    /// - true: if read only
    FlightSqlServerReadOnly = 3,
    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports executing
    /// SQL queries.
    ///
    /// Note that the absence of this info (as opposed to a false value) does not necessarily
    /// mean that SQL is not supported, as this property was not originally defined.
    FlightSqlServerSql = 4,
    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports executing
    /// Substrait plans.
    FlightSqlServerSubstrait = 5,
    ///
    /// Retrieves a string value indicating the minimum supported Substrait version, or null
    /// if Substrait is not supported.
    FlightSqlServerSubstraitMinVersion = 6,
    ///
    /// Retrieves a string value indicating the maximum supported Substrait version, or null
    /// if Substrait is not supported.
    FlightSqlServerSubstraitMaxVersion = 7,
    ///
    /// Retrieves an int32 indicating whether the Flight SQL Server supports the
    /// BeginTransaction/EndTransaction/BeginSavepoint/EndSavepoint actions.
    ///
    /// Even if this is not supported, the database may still support explicit "BEGIN
    /// TRANSACTION"/"COMMIT" SQL statements (see SQL_TRANSACTIONS_SUPPORTED); this property
    /// is only about whether the server implements the Flight SQL API endpoints.
    ///
    /// The possible values are listed in `SqlSupportedTransaction`.
    FlightSqlServerTransaction = 8,
    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports explicit
    /// query cancellation (the CancelQuery action).
    FlightSqlServerCancel = 9,
    ///
    /// Retrieves an int32 indicating the timeout (in milliseconds) for prepared statement handles.
    ///
    /// If 0, there is no timeout.  Servers should reset the timeout when the handle is used in a command.
    FlightSqlServerStatementTimeout = 100,
    ///
    /// Retrieves an int32 indicating the timeout (in milliseconds) for transactions, since transactions are not tied to a connection.
    ///
    /// If 0, there is no timeout.  Servers should reset the timeout when the handle is used in a command.
    FlightSqlServerTransactionTimeout = 101,
    // SQL Syntax Information [500-1000): provides information about SQL syntax supported by the Flight SQL Server.

    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports CREATE and DROP of catalogs.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of catalogs.
    /// - true: if it supports CREATE and DROP of catalogs.
    SqlDdlCatalog = 500,
    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports CREATE and DROP of schemas.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of schemas.
    /// - true: if it supports CREATE and DROP of schemas.
    SqlDdlSchema = 501,
    ///
    /// Indicates whether the Flight SQL Server supports CREATE and DROP of tables.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of tables.
    /// - true: if it supports CREATE and DROP of tables.
    SqlDdlTable = 502,
    ///
    /// Retrieves a int32 ordinal representing the case sensitivity of catalog, table, schema and table names.
    ///
    /// The possible values are listed in `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
    SqlIdentifierCase = 503,
    /// Retrieves a UTF-8 string with the supported character(s) used to surround a delimited identifier.
    SqlIdentifierQuoteChar = 504,
    ///
    /// Retrieves a int32 describing the case sensitivity of quoted identifiers.
    ///
    /// The possible values are listed in `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
    SqlQuotedIdentifierCase = 505,
    ///
    /// Retrieves a boolean value indicating whether all tables are selectable.
    ///
    /// Returns:
    /// - false: if not all tables are selectable or if none are;
    /// - true: if all tables are selectable.
    SqlAllTablesAreSelectable = 506,
    ///
    /// Retrieves the null ordering.
    ///
    /// Returns a int32 ordinal for the null ordering being used, as described in
    /// `arrow.flight.protocol.sql.SqlNullOrdering`.
    SqlNullOrdering = 507,
    /// Retrieves a UTF-8 string list with values of the supported keywords.
    SqlKeywords = 508,
    /// Retrieves a UTF-8 string list with values of the supported numeric functions.
    SqlNumericFunctions = 509,
    /// Retrieves a UTF-8 string list with values of the supported string functions.
    SqlStringFunctions = 510,
    /// Retrieves a UTF-8 string list with values of the supported system functions.
    SqlSystemFunctions = 511,
    /// Retrieves a UTF-8 string list with values of the supported datetime functions.
    SqlDatetimeFunctions = 512,
    ///
    /// Retrieves the UTF-8 string that can be used to escape wildcard characters.
    /// This is the string that can be used to escape '_' or '%' in the catalog search parameters that are a pattern
    /// (and therefore use one of the wildcard characters).
    /// The '_' character represents any single character; the '%' character represents any sequence of zero or more
    /// characters.
    SqlSearchStringEscape = 513,
    ///
    /// Retrieves a UTF-8 string with all the "extra" characters that can be used in unquoted identifier names
    /// (those beyond a-z, A-Z, 0-9 and _).
    SqlExtraNameCharacters = 514,
    ///
    /// Retrieves a boolean value indicating whether column aliasing is supported.
    /// If so, the SQL AS clause can be used to provide names for computed columns or to provide alias names for columns
    /// as required.
    ///
    /// Returns:
    /// - false: if column aliasing is unsupported;
    /// - true: if column aliasing is supported.
    SqlSupportsColumnAliasing = 515,
    ///
    /// Retrieves a boolean value indicating whether concatenations between null and non-null values being
    /// null are supported.
    ///
    /// - Returns:
    /// - false: if concatenations between null and non-null values being null are unsupported;
    /// - true: if concatenations between null and non-null values being null are supported.
    SqlNullPlusNullIsNull = 516,
    ///
    /// Retrieves a map where the key is the type to convert from and the value is a list with the types to convert to,
    /// indicating the supported conversions. Each key and each item on the list value is a value to a predefined type on
    /// SqlSupportsConvert enum.
    /// The returned map will be:  map<int32, list<int32>>
    SqlSupportsConvert = 517,
    ///
    /// Retrieves a boolean value indicating whether, when table correlation names are supported,
    /// they are restricted to being different from the names of the tables.
    ///
    /// Returns:
    /// - false: if table correlation names are unsupported;
    /// - true: if table correlation names are supported.
    SqlSupportsTableCorrelationNames = 518,
    ///
    /// Retrieves a boolean value indicating whether, when table correlation names are supported,
    /// they are restricted to being different from the names of the tables.
    ///
    /// Returns:
    /// - false: if different table correlation names are unsupported;
    /// - true: if different table correlation names are supported
    SqlSupportsDifferentTableCorrelationNames = 519,
    ///
    /// Retrieves a boolean value indicating whether expressions in ORDER BY lists are supported.
    ///
    /// Returns:
    /// - false: if expressions in ORDER BY are unsupported;
    /// - true: if expressions in ORDER BY are supported;
    SqlSupportsExpressionsInOrderBy = 520,
    ///
    /// Retrieves a boolean value indicating whether using a column that is not in the SELECT statement in a GROUP BY
    /// clause is supported.
    ///
    /// Returns:
    /// - false: if using a column that is not in the SELECT statement in a GROUP BY clause is unsupported;
    /// - true: if using a column that is not in the SELECT statement in a GROUP BY clause is supported.
    SqlSupportsOrderByUnrelated = 521,
    ///
    /// Retrieves the supported GROUP BY commands;
    ///
    /// Returns an int32 bitmask value representing the supported commands.
    /// The returned bitmask should be parsed in order to retrieve the supported commands.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (GROUP BY is unsupported);
    /// - return 1 (\b1)   => \[SQL_GROUP_BY_UNRELATED\];
    /// - return 2 (\b10)  => \[SQL_GROUP_BY_BEYOND_SELECT\];
    /// - return 3 (\b11)  => [SQL_GROUP_BY_UNRELATED, SQL_GROUP_BY_BEYOND_SELECT].
    /// Valid GROUP BY types are described under `arrow.flight.protocol.sql.SqlSupportedGroupBy`.
    SqlSupportedGroupBy = 522,
    ///
    /// Retrieves a boolean value indicating whether specifying a LIKE escape clause is supported.
    ///
    /// Returns:
    /// - false: if specifying a LIKE escape clause is unsupported;
    /// - true: if specifying a LIKE escape clause is supported.
    SqlSupportsLikeEscapeClause = 523,
    ///
    /// Retrieves a boolean value indicating whether columns may be defined as non-nullable.
    ///
    /// Returns:
    /// - false: if columns cannot be defined as non-nullable;
    /// - true: if columns may be defined as non-nullable.
    SqlSupportsNonNullableColumns = 524,
    ///
    /// Retrieves the supported SQL grammar level as per the ODBC specification.
    ///
    /// Returns an int32 bitmask value representing the supported SQL grammar level.
    /// The returned bitmask should be parsed in order to retrieve the supported grammar levels.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (SQL grammar is unsupported);
    /// - return 1 (\b1)   => \[SQL_MINIMUM_GRAMMAR\];
    /// - return 2 (\b10)  => \[SQL_CORE_GRAMMAR\];
    /// - return 3 (\b11)  => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR];
    /// - return 4 (\b100) => \[SQL_EXTENDED_GRAMMAR\];
    /// - return 5 (\b101) => [SQL_MINIMUM_GRAMMAR, SQL_EXTENDED_GRAMMAR];
    /// - return 6 (\b110) => [SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR];
    /// - return 7 (\b111) => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR].
    /// Valid SQL grammar levels are described under `arrow.flight.protocol.sql.SupportedSqlGrammar`.
    SqlSupportedGrammar = 525,
    ///
    /// Retrieves the supported ANSI92 SQL grammar level.
    ///
    /// Returns an int32 bitmask value representing the supported ANSI92 SQL grammar level.
    /// The returned bitmask should be parsed in order to retrieve the supported commands.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (ANSI92 SQL grammar is unsupported);
    /// - return 1 (\b1)   => \[ANSI92_ENTRY_SQL\];
    /// - return 2 (\b10)  => \[ANSI92_INTERMEDIATE_SQL\];
    /// - return 3 (\b11)  => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL];
    /// - return 4 (\b100) => \[ANSI92_FULL_SQL\];
    /// - return 5 (\b101) => [ANSI92_ENTRY_SQL, ANSI92_FULL_SQL];
    /// - return 6 (\b110) => [ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL];
    /// - return 7 (\b111) => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL].
    /// Valid ANSI92 SQL grammar levels are described under `arrow.flight.protocol.sql.SupportedAnsi92SqlGrammarLevel`.
    SqlAnsi92SupportedLevel = 526,
    ///
    /// Retrieves a boolean value indicating whether the SQL Integrity Enhancement Facility is supported.
    ///
    /// Returns:
    /// - false: if the SQL Integrity Enhancement Facility is supported;
    /// - true: if the SQL Integrity Enhancement Facility is supported.
    SqlSupportsIntegrityEnhancementFacility = 527,
    ///
    /// Retrieves the support level for SQL OUTER JOINs.
    ///
    /// Returns a int32 ordinal for the SQL ordering being used, as described in
    /// `arrow.flight.protocol.sql.SqlOuterJoinsSupportLevel`.
    SqlOuterJoinsSupportLevel = 528,
    /// Retrieves a UTF-8 string with the preferred term for "schema".
    SqlSchemaTerm = 529,
    /// Retrieves a UTF-8 string with the preferred term for "procedure".
    SqlProcedureTerm = 530,
    ///
    /// Retrieves a UTF-8 string with the preferred term for "catalog".
    /// If a empty string is returned its assumed that the server does NOT supports catalogs.
    SqlCatalogTerm = 531,
    ///
    /// Retrieves a boolean value indicating whether a catalog appears at the start of a fully qualified table name.
    ///
    /// - false: if a catalog does not appear at the start of a fully qualified table name;
    /// - true: if a catalog appears at the start of a fully qualified table name.
    SqlCatalogAtStart = 532,
    ///
    /// Retrieves the supported actions for a SQL schema.
    ///
    /// Returns an int32 bitmask value representing the supported actions for a SQL schema.
    /// The returned bitmask should be parsed in order to retrieve the supported actions for a SQL schema.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported actions for SQL schema);
    /// - return 1 (\b1)   => \[SQL_ELEMENT_IN_PROCEDURE_CALLS\];
    /// - return 2 (\b10)  => \[SQL_ELEMENT_IN_INDEX_DEFINITIONS\];
    /// - return 3 (\b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 4 (\b100) => \[SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS\];
    /// - return 5 (\b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 6 (\b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 7 (\b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
    /// Valid actions for a SQL schema described under `arrow.flight.protocol.sql.SqlSupportedElementActions`.
    SqlSchemasSupportedActions = 533,
    ///
    /// Retrieves the supported actions for a SQL schema.
    ///
    /// Returns an int32 bitmask value representing the supported actions for a SQL catalog.
    /// The returned bitmask should be parsed in order to retrieve the supported actions for a SQL catalog.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported actions for SQL catalog);
    /// - return 1 (\b1)   => \[SQL_ELEMENT_IN_PROCEDURE_CALLS\];
    /// - return 2 (\b10)  => \[SQL_ELEMENT_IN_INDEX_DEFINITIONS\];
    /// - return 3 (\b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 4 (\b100) => \[SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS\];
    /// - return 5 (\b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 6 (\b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 7 (\b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
    /// Valid actions for a SQL catalog are described under `arrow.flight.protocol.sql.SqlSupportedElementActions`.
    SqlCatalogsSupportedActions = 534,
    ///
    /// Retrieves the supported SQL positioned commands.
    ///
    /// Returns an int32 bitmask value representing the supported SQL positioned commands.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL positioned commands.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported SQL positioned commands);
    /// - return 1 (\b1)   => \[SQL_POSITIONED_DELETE\];
    /// - return 2 (\b10)  => \[SQL_POSITIONED_UPDATE\];
    /// - return 3 (\b11)  => [SQL_POSITIONED_DELETE, SQL_POSITIONED_UPDATE].
    /// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlSupportedPositionedCommands`.
    SqlSupportedPositionedCommands = 535,
    ///
    /// Retrieves a boolean value indicating whether SELECT FOR UPDATE statements are supported.
    ///
    /// Returns:
    /// - false: if SELECT FOR UPDATE statements are unsupported;
    /// - true: if SELECT FOR UPDATE statements are supported.
    SqlSelectForUpdateSupported = 536,
    ///
    /// Retrieves a boolean value indicating whether stored procedure calls that use the stored procedure escape syntax
    /// are supported.
    ///
    /// Returns:
    /// - false: if stored procedure calls that use the stored procedure escape syntax are unsupported;
    /// - true: if stored procedure calls that use the stored procedure escape syntax are supported.
    SqlStoredProceduresSupported = 537,
    ///
    /// Retrieves the supported SQL subqueries.
    ///
    /// Returns an int32 bitmask value representing the supported SQL subqueries.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL subqueries.
    ///
    /// For instance:
    /// - return 0   (\b0)     => [] (no supported SQL subqueries);
    /// - return 1   (\b1)     => \[SQL_SUBQUERIES_IN_COMPARISONS\];
    /// - return 2   (\b10)    => \[SQL_SUBQUERIES_IN_EXISTS\];
    /// - return 3   (\b11)    => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS];
    /// - return 4   (\b100)   => \[SQL_SUBQUERIES_IN_INS\];
    /// - return 5   (\b101)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS];
    /// - return 6   (\b110)   => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_EXISTS];
    /// - return 7   (\b111)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS];
    /// - return 8   (\b1000)  => \[SQL_SUBQUERIES_IN_QUANTIFIEDS\];
    /// - return 9   (\b1001)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 10  (\b1010)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 11  (\b1011)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 12  (\b1100)  => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 13  (\b1101)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 14  (\b1110)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 15  (\b1111)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - ...
    /// Valid SQL subqueries are described under `arrow.flight.protocol.sql.SqlSupportedSubqueries`.
    SqlSupportedSubqueries = 538,
    ///
    /// Retrieves a boolean value indicating whether correlated subqueries are supported.
    ///
    /// Returns:
    /// - false: if correlated subqueries are unsupported;
    /// - true: if correlated subqueries are supported.
    SqlCorrelatedSubqueriesSupported = 539,
    ///
    /// Retrieves the supported SQL UNIONs.
    ///
    /// Returns an int32 bitmask value representing the supported SQL UNIONs.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL UNIONs.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported SQL positioned commands);
    /// - return 1 (\b1)   => \[SQL_UNION\];
    /// - return 2 (\b10)  => \[SQL_UNION_ALL\];
    /// - return 3 (\b11)  => [SQL_UNION, SQL_UNION_ALL].
    /// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlSupportedUnions`.
    SqlSupportedUnions = 540,
    /// Retrieves a int64 value representing the maximum number of hex characters allowed in an inline binary literal.
    SqlMaxBinaryLiteralLength = 541,
    /// Retrieves a int64 value representing the maximum number of characters allowed for a character literal.
    SqlMaxCharLiteralLength = 542,
    /// Retrieves a int64 value representing the maximum number of characters allowed for a column name.
    SqlMaxColumnNameLength = 543,
    /// Retrieves a int64 value representing the the maximum number of columns allowed in a GROUP BY clause.
    SqlMaxColumnsInGroupBy = 544,
    /// Retrieves a int64 value representing the maximum number of columns allowed in an index.
    SqlMaxColumnsInIndex = 545,
    /// Retrieves a int64 value representing the maximum number of columns allowed in an ORDER BY clause.
    SqlMaxColumnsInOrderBy = 546,
    /// Retrieves a int64 value representing the maximum number of columns allowed in a SELECT list.
    SqlMaxColumnsInSelect = 547,
    /// Retrieves a int64 value representing the maximum number of columns allowed in a table.
    SqlMaxColumnsInTable = 548,
    /// Retrieves a int64 value representing the maximum number of concurrent connections possible.
    SqlMaxConnections = 549,
    /// Retrieves a int64 value the maximum number of characters allowed in a cursor name.
    SqlMaxCursorNameLength = 550,
    ///
    /// Retrieves a int64 value representing the maximum number of bytes allowed for an index,
    /// including all of the parts of the index.
    SqlMaxIndexLength = 551,
    /// Retrieves a int64 value representing the maximum number of characters allowed in a schema name.
    SqlDbSchemaNameLength = 552,
    /// Retrieves a int64 value representing the maximum number of characters allowed in a procedure name.
    SqlMaxProcedureNameLength = 553,
    /// Retrieves a int64 value representing the maximum number of characters allowed in a catalog name.
    SqlMaxCatalogNameLength = 554,
    /// Retrieves a int64 value representing the maximum number of bytes allowed in a single row.
    SqlMaxRowSize = 555,
    ///
    /// Retrieves a boolean indicating whether the return value for the JDBC method getMaxRowSize includes the SQL
    /// data types LONGVARCHAR and LONGVARBINARY.
    ///
    /// Returns:
    /// - false: if return value for the JDBC method getMaxRowSize does
    ///          not include the SQL data types LONGVARCHAR and LONGVARBINARY;
    /// - true: if return value for the JDBC method getMaxRowSize includes
    ///         the SQL data types LONGVARCHAR and LONGVARBINARY.
    SqlMaxRowSizeIncludesBlobs = 556,
    ///
    /// Retrieves a int64 value representing the maximum number of characters allowed for an SQL statement;
    /// a result of 0 (zero) means that there is no limit or the limit is not known.
    SqlMaxStatementLength = 557,
    /// Retrieves a int64 value representing the maximum number of active statements that can be open at the same time.
    SqlMaxStatements = 558,
    /// Retrieves a int64 value representing the maximum number of characters allowed in a table name.
    SqlMaxTableNameLength = 559,
    /// Retrieves a int64 value representing the maximum number of tables allowed in a SELECT statement.
    SqlMaxTablesInSelect = 560,
    /// Retrieves a int64 value representing the maximum number of characters allowed in a user name.
    SqlMaxUsernameLength = 561,
    ///
    /// Retrieves this database's default transaction isolation level as described in
    /// `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
    ///
    /// Returns a int32 ordinal for the SQL transaction isolation level.
    SqlDefaultTransactionIsolation = 562,
    ///
    /// Retrieves a boolean value indicating whether transactions are supported. If not, invoking the method commit is a
    /// noop, and the isolation level is `arrow.flight.protocol.sql.SqlTransactionIsolationLevel.TRANSACTION_NONE`.
    ///
    /// Returns:
    /// - false: if transactions are unsupported;
    /// - true: if transactions are supported.
    SqlTransactionsSupported = 563,
    ///
    /// Retrieves the supported transactions isolation levels.
    ///
    /// Returns an int32 bitmask value representing the supported transactions isolation levels.
    /// The returned bitmask should be parsed in order to retrieve the supported transactions isolation levels.
    ///
    /// For instance:
    /// - return 0   (\b0)     => [] (no supported SQL transactions isolation levels);
    /// - return 1   (\b1)     => \[SQL_TRANSACTION_NONE\];
    /// - return 2   (\b10)    => \[SQL_TRANSACTION_READ_UNCOMMITTED\];
    /// - return 3   (\b11)    => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED];
    /// - return 4   (\b100)   => \[SQL_TRANSACTION_REPEATABLE_READ\];
    /// - return 5   (\b101)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 6   (\b110)   => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 7   (\b111)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 8   (\b1000)  => \[SQL_TRANSACTION_REPEATABLE_READ\];
    /// - return 9   (\b1001)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 10  (\b1010)  => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 11  (\b1011)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 12  (\b1100)  => [SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 13  (\b1101)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 14  (\b1110)  => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 15  (\b1111)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 16  (\b10000) => \[SQL_TRANSACTION_SERIALIZABLE\];
    /// - ...
    /// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
    SqlSupportedTransactionsIsolationLevels = 564,
    ///
    /// Retrieves a boolean value indicating whether a data definition statement within a transaction forces
    /// the transaction to commit.
    ///
    /// Returns:
    /// - false: if a data definition statement within a transaction does not force the transaction to commit;
    /// - true: if a data definition statement within a transaction forces the transaction to commit.
    SqlDataDefinitionCausesTransactionCommit = 565,
    ///
    /// Retrieves a boolean value indicating whether a data definition statement within a transaction is ignored.
    ///
    /// Returns:
    /// - false: if a data definition statement within a transaction is taken into account;
    /// - true: a data definition statement within a transaction is ignored.
    SqlDataDefinitionsInTransactionsIgnored = 566,
    ///
    /// Retrieves an int32 bitmask value representing the supported result set types.
    /// The returned bitmask should be parsed in order to retrieve the supported result set types.
    ///
    /// For instance:
    /// - return 0   (\b0)     => [] (no supported result set types);
    /// - return 1   (\b1)     => \[SQL_RESULT_SET_TYPE_UNSPECIFIED\];
    /// - return 2   (\b10)    => \[SQL_RESULT_SET_TYPE_FORWARD_ONLY\];
    /// - return 3   (\b11)    => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_FORWARD_ONLY];
    /// - return 4   (\b100)   => \[SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE\];
    /// - return 5   (\b101)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 6   (\b110)   => [SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 7   (\b111)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 8   (\b1000)  => \[SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE\];
    /// - ...
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetType`.
    SqlSupportedResultSetTypes = 567,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_UNSPECIFIED`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetUnspecified = 568,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetForwardOnly = 569,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetScrollSensitive = 570,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetScrollInsensitive = 571,
    ///
    /// Retrieves a boolean value indicating whether this database supports batch updates.
    ///
    /// - false: if this database does not support batch updates;
    /// - true: if this database supports batch updates.
    SqlBatchUpdatesSupported = 572,
    ///
    /// Retrieves a boolean value indicating whether this database supports savepoints.
    ///
    /// Returns:
    /// - false: if this database does not support savepoints;
    /// - true: if this database supports savepoints.
    SqlSavepointsSupported = 573,
    ///
    /// Retrieves a boolean value indicating whether named parameters are supported in callable statements.
    ///
    /// Returns:
    /// - false: if named parameters in callable statements are unsupported;
    /// - true: if named parameters in callable statements are supported.
    SqlNamedParametersSupported = 574,
    ///
    /// Retrieves a boolean value indicating whether updates made to a LOB are made on a copy or directly to the LOB.
    ///
    /// Returns:
    /// - false: if updates made to a LOB are made directly to the LOB;
    /// - true: if updates made to a LOB are made on a copy.
    SqlLocatorsUpdateCopy = 575,
    ///
    /// Retrieves a boolean value indicating whether invoking user-defined or vendor functions
    /// using the stored procedure escape syntax is supported.
    ///
    /// Returns:
    /// - false: if invoking user-defined or vendor functions using the stored procedure escape syntax is unsupported;
    /// - true: if invoking user-defined or vendor functions using the stored procedure escape syntax is supported.
    SqlStoredFunctionsUsingCallSyntaxSupported = 576,
}
/// The level of support for Flight SQL transaction RPCs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedTransaction {
    /// Unknown/not indicated/no support
    None = 0,
    /// Transactions, but not savepoints.
    /// A savepoint is a mark within a transaction that can be individually
    /// rolled back to. Not all databases support savepoints.
    Transaction = 1,
    /// Transactions and savepoints
    Savepoint = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedCaseSensitivity {
    SqlCaseSensitivityUnknown = 0,
    SqlCaseSensitivityCaseInsensitive = 1,
    SqlCaseSensitivityUppercase = 2,
    SqlCaseSensitivityLowercase = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlNullOrdering {
    SqlNullsSortedHigh = 0,
    SqlNullsSortedLow = 1,
    SqlNullsSortedAtStart = 2,
    SqlNullsSortedAtEnd = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SupportedSqlGrammar {
    SqlMinimumGrammar = 0,
    SqlCoreGrammar = 1,
    SqlExtendedGrammar = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SupportedAnsi92SqlGrammarLevel {
    Ansi92EntrySql = 0,
    Ansi92IntermediateSql = 1,
    Ansi92FullSql = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlOuterJoinsSupportLevel {
    SqlJoinsUnsupported = 0,
    SqlLimitedOuterJoins = 1,
    SqlFullOuterJoins = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedGroupBy {
    SqlGroupByUnrelated = 0,
    SqlGroupByBeyondSelect = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedElementActions {
    SqlElementInProcedureCalls = 0,
    SqlElementInIndexDefinitions = 1,
    SqlElementInPrivilegeDefinitions = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedPositionedCommands {
    SqlPositionedDelete = 0,
    SqlPositionedUpdate = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedSubqueries {
    SqlSubqueriesInComparisons = 0,
    SqlSubqueriesInExists = 1,
    SqlSubqueriesInIns = 2,
    SqlSubqueriesInQuantifieds = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedUnions {
    SqlUnion = 0,
    SqlUnionAll = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlTransactionIsolationLevel {
    SqlTransactionNone = 0,
    SqlTransactionReadUncommitted = 1,
    SqlTransactionReadCommitted = 2,
    SqlTransactionRepeatableRead = 3,
    SqlTransactionSerializable = 4,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedTransactions {
    SqlTransactionUnspecified = 0,
    SqlDataDefinitionTransactions = 1,
    SqlDataManipulationTransactions = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedResultSetType {
    SqlResultSetTypeUnspecified = 0,
    SqlResultSetTypeForwardOnly = 1,
    SqlResultSetTypeScrollInsensitive = 2,
    SqlResultSetTypeScrollSensitive = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedResultSetConcurrency {
    SqlResultSetConcurrencyUnspecified = 0,
    SqlResultSetConcurrencyReadOnly = 1,
    SqlResultSetConcurrencyUpdatable = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportsConvert {
    SqlConvertBigint = 0,
    SqlConvertBinary = 1,
    SqlConvertBit = 2,
    SqlConvertChar = 3,
    SqlConvertDate = 4,
    SqlConvertDecimal = 5,
    SqlConvertFloat = 6,
    SqlConvertInteger = 7,
    SqlConvertIntervalDayTime = 8,
    SqlConvertIntervalYearMonth = 9,
    SqlConvertLongvarbinary = 10,
    SqlConvertLongvarchar = 11,
    SqlConvertNumeric = 12,
    SqlConvertReal = 13,
    SqlConvertSmallint = 14,
    SqlConvertTime = 15,
    SqlConvertTimestamp = 16,
    SqlConvertTinyint = 17,
    SqlConvertVarbinary = 18,
    SqlConvertVarchar = 19,
}
///*
/// The JDBC/ODBC-defined type of any object.
/// All the values here are the sames as in the JDBC and ODBC specs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum XdbcDataType {
    XdbcUnknownType = 0,
    XdbcChar = 1,
    XdbcNumeric = 2,
    XdbcDecimal = 3,
    XdbcInteger = 4,
    XdbcSmallint = 5,
    XdbcFloat = 6,
    XdbcReal = 7,
    XdbcDouble = 8,
    XdbcDatetime = 9,
    XdbcInterval = 10,
    XdbcVarchar = 12,
    XdbcDate = 91,
    XdbcTime = 92,
    XdbcTimestamp = 93,
    XdbcLongvarchar = -1,
    XdbcBinary = -2,
    XdbcVarbinary = -3,
    XdbcLongvarbinary = -4,
    XdbcBigint = -5,
    XdbcTinyint = -6,
    XdbcBit = -7,
    XdbcWchar = -8,
    XdbcWvarchar = -9,
}
///*
/// Detailed subtype information for XDBC_TYPE_DATETIME and XDBC_TYPE_INTERVAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum XdbcDatetimeSubcode {
    XdbcSubcodeUnknown = 0,
    XdbcSubcodeYear = 1,
    XdbcSubcodeTime = 2,
    XdbcSubcodeTimestamp = 3,
    XdbcSubcodeTimeWithTimezone = 4,
    XdbcSubcodeTimestampWithTimezone = 5,
    XdbcSubcodeSecond = 6,
    XdbcSubcodeYearToMonth = 7,
    XdbcSubcodeDayToHour = 8,
    XdbcSubcodeDayToMinute = 9,
    XdbcSubcodeDayToSecond = 10,
    XdbcSubcodeHourToMinute = 11,
    XdbcSubcodeHourToSecond = 12,
    XdbcSubcodeMinuteToSecond = 13,
    XdbcSubcodeIntervalYear = 101,
    XdbcSubcodeIntervalMonth = 102,
    XdbcSubcodeIntervalDay = 103,
    XdbcSubcodeIntervalHour = 104,
    XdbcSubcodeIntervalMinute = 105,
    XdbcSubcodeIntervalSecond = 106,
    XdbcSubcodeIntervalYearToMonth = 107,
    XdbcSubcodeIntervalDayToHour = 108,
    XdbcSubcodeIntervalDayToMinute = 109,
    XdbcSubcodeIntervalDayToSecond = 110,
    XdbcSubcodeIntervalHourToMinute = 111,
    XdbcSubcodeIntervalHourToSecond = 112,
    XdbcSubcodeIntervalMinuteToSecond = 113,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Nullable {
    ///*
    /// Indicates that the fields does not allow the use of null values.
    NullabilityNoNulls = 0,
    ///*
    /// Indicates that the fields allow the use of null values.
    NullabilityNullable = 1,
    ///*
    /// Indicates that nullability of the fields can not be determined.
    NullabilityUnknown = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Searchable {
    ///*
    /// Indicates that column can not be used in a WHERE clause.
    None = 0,
    ///*
    /// Indicates that the column can be used in a WHERE clause if it is using a
    /// LIKE operator.
    Char = 1,
    ///*
    /// Indicates that the column can be used In a WHERE clause with any
    /// operator other than LIKE.
    ///
    /// - Allowed operators: comparison, quantified comparison, BETWEEN,
    ///                      DISTINCT, IN, MATCH, and UNIQUE.
    Basic = 2,
    ///*
    /// Indicates that the column can be used in a WHERE clause using any operator.
    Full = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UpdateDeleteRules {
    Cascade = 0,
    Restrict = 1,
    SetNull = 2,
    NoAction = 3,
    SetDefault = 4,
}
