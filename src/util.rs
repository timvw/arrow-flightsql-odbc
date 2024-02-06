use crate::arrow_flight_protocol_sql::*;
use arrow::datatypes::Schema;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::writer::{EncodedData, IpcDataGenerator, IpcWriteOptions};
use prost::Message;
use std::ops::Deref;
use crate::arrow_flight_protocol::FlightData;
use arrow::ipc::writer;
use arrow::record_batch::RecordBatch;

/// ProstMessageExt are useful utility methods for prost::Message types
pub trait ProstMessageExt: prost::Message + Default {
    /// type_url for this Message
    fn type_url() -> &'static str;

    /// Convert this Message to prost_types::Any
    fn as_any(&self) -> prost_types::Any;
}

macro_rules! prost_message_ext {
    ($($name:ty,)*) => {
        $(
            impl ProstMessageExt for $name {
                fn type_url() -> &'static str {
                    concat!("type.googleapis.com/arrow.flight.protocol.sql.", stringify!($name))
                }

                fn as_any(&self) -> prost_types::Any {
                    prost_types::Any {
                        type_url: <$name>::type_url().to_string(),
                        value: self.encode_to_vec(),
                    }
                }
            }
        )*
    };
}

// Implement ProstMessageExt for all structs defined in FlightSql.proto
prost_message_ext!(
    ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult,
    CommandGetCatalogs,
    CommandGetCrossReference,
    CommandGetDbSchemas,
    CommandGetExportedKeys,
    CommandGetImportedKeys,
    CommandGetPrimaryKeys,
    CommandGetSqlInfo,
    CommandGetTableTypes,
    CommandGetTables,
    CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate,
    CommandStatementQuery,
    CommandStatementUpdate,
    DoPutUpdateResult,
    TicketStatementQuery,
);

/// ProstAnyExt are useful utility methods for prost_types::Any
/// The API design is inspired by [rust-protobuf](https://github.com/stepancheg/rust-protobuf/blob/master/protobuf/src/well_known_types_util/any.rs)
pub trait ProstAnyExt {
    /// Check if `Any` contains a message of given type.
    fn is<M: ProstMessageExt>(&self) -> bool;

    /// Extract a message from this `Any`.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` when message type mismatch
    /// * `Err` when parse failed
    fn unpack<M: ProstMessageExt>(&self) -> ArrowResult<Option<M>>;

    /// Pack any message into `prost_types::Any` value.
    fn pack<M: ProstMessageExt>(message: &M) -> ArrowResult<prost_types::Any>;
}

impl ProstAnyExt for prost_types::Any {
    fn is<M: ProstMessageExt>(&self) -> bool {
        M::type_url() == self.type_url
    }

    fn unpack<M: ProstMessageExt>(&self) -> ArrowResult<Option<M>> {
        if !self.is::<M>() {
            return Ok(None);
        }
        let m = prost::Message::decode(&*self.value).map_err(|err| {
            ArrowError::ParseError(format!("Unable to decode Any value: {}", err))
        })?;
        Ok(Some(m))
    }

    fn pack<M: ProstMessageExt>(message: &M) -> ArrowResult<prost_types::Any> {
        Ok(message.as_any())
    }
}

/// SchemaAsIpc represents a pairing of a `Schema` with IpcWriteOptions
pub struct SchemaAsIpc<'a> {
    pub pair: (&'a Schema, &'a IpcWriteOptions),
}

impl<'a> SchemaAsIpc<'a> {
    pub fn new(schema: &'a Schema, options: &'a IpcWriteOptions) -> Self {
        SchemaAsIpc {
            pair: (schema, options),
        }
    }
}

/// IpcMessage represents a `Schema` in the format expected in
/// `FlightInfo.schema`
#[derive(Debug)]
pub struct IpcMessage(pub Vec<u8>);

fn flight_schema_as_encoded_data(arrow_schema: &Schema, options: &IpcWriteOptions) -> EncodedData {
    let data_gen = IpcDataGenerator::default();
    data_gen.schema_to_bytes(arrow_schema, options)
}

fn flight_schema_as_flatbuffer(schema: &Schema, options: &IpcWriteOptions) -> IpcMessage {
    let encoded_data = flight_schema_as_encoded_data(schema, options);
    IpcMessage(encoded_data.ipc_message.into())
}

impl From<SchemaAsIpc<'_>> for FlightData {
    fn from(schema_ipc: SchemaAsIpc) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(schema_ipc.0, schema_ipc.1);
        FlightData {
            data_header: vals,
            ..Default::default()
        }
    }
}

impl TryFrom<SchemaAsIpc<'_>> for IpcMessage {
    type Error = ArrowError;

    fn try_from(schema_ipc: SchemaAsIpc) -> ArrowResult<Self> {
        let pair = *schema_ipc;
        let encoded_data = flight_schema_as_encoded_data(pair.0, pair.1);

        let mut schema = vec![];
        arrow::ipc::writer::write_message(&mut schema, encoded_data, pair.1)?;
        Ok(IpcMessage(schema))
    }
}

impl Deref for IpcMessage {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Deref for SchemaAsIpc<'a> {
    type Target = (&'a Schema, &'a IpcWriteOptions);

    fn deref(&self) -> &Self::Target {
        &self.pair
    }
}

// Convert `RecordBatch`es to wire protocol `FlightData`s
pub fn batches_to_flight_data(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<Vec<FlightData>, ArrowError>
{
    let options = IpcWriteOptions::default();
    let schema_flight_data: FlightData = crate::util::SchemaAsIpc::new(schema, &options).into();
    let mut dictionaries = vec![];
    let mut flight_data = vec![];

    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker = writer::DictionaryTracker::new(false);

    for batch in batches.iter() {
        let (encoded_dictionaries, encoded_batch) =
            data_gen.encoded_batch(batch, &mut dictionary_tracker, &options)?;

        dictionaries.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }
    let mut stream = vec![schema_flight_data];
    stream.extend(dictionaries);
    stream.extend(flight_data);
    let flight_data: Vec<_> = stream.into_iter().collect();
    Ok(flight_data)
}
