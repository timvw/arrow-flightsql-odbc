use tonic::{Request, Response, Status, Streaming};
use crate::arrow_flight_protocol::flight_service_server::FlightService;
use crate::arrow_flight_protocol::{Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaResult, Ticket, PutResult, Result as ActionResult, ActionType, FlightEndpoint};
use futures_core::Stream;
use std::pin::Pin;
use arrow::datatypes::Schema;
use tokio::sync::{mpsc, mpsc::Receiver, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use crate::arrow_flight_protocol_sql::*;
use prost::Message;
use prost_types::Any;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc;
use arrow::ipc::convert::schema_to_fb;
use arrow_odbc::{odbc_api, OdbcReader};
use arrow_odbc::odbc_api::{CursorImpl, Environment};
use tokio::sync::mpsc::Sender;
use arrow::ipc::writer::IpcWriteOptions;
use core::ops::Deref;

#[derive(Debug)]
pub enum MyServerError {
    OdbcApiError(odbc_api::Error),
    TonicTransportError(tonic::transport::Error),
    TonicReflectionServerError(tonic_reflection::server::Error),
    AddrParseError(std::net::AddrParseError),
    ArrowOdbcError(arrow_odbc::Error),
    TonicStatus(tonic::Status),
}

impl From<odbc_api::Error> for MyServerError {
    fn from(error: odbc_api::Error) -> Self {
        MyServerError::OdbcApiError(error)
    }
}

impl From<tonic::transport::Error> for MyServerError {
    fn from(error: tonic::transport::Error) -> Self {
        MyServerError::TonicTransportError(error)
    }
}

impl From<tonic_reflection::server::Error> for MyServerError {
    fn from(error: tonic_reflection::server::Error) -> Self {
        MyServerError::TonicReflectionServerError(error)
    }
}

impl From<std::net::AddrParseError> for MyServerError {
    fn from(error: std::net::AddrParseError) -> Self {
        MyServerError::AddrParseError(error)
    }
}

impl From<arrow_odbc::Error> for MyServerError {
    fn from(error: arrow_odbc::Error) -> Self {
        MyServerError::ArrowOdbcError(error)
    }
}

impl From<tonic::Status> for MyServerError {
    fn from(error: tonic::Status) -> Self {
        MyServerError::TonicStatus(error)
    }
}

#[derive(Debug)]
pub enum OdbcCommand {
    Query(OdbcQueryRequest)
}

#[derive(Debug)]
pub struct OdbcQueryRequest {
    query: String,
    response_sender: oneshot::Sender<OdbcQueryResponse>,
}

#[derive(Clone, Debug)]
pub struct OdbcQueryResponse {
    schema: Schema,
}

#[derive(Debug, Clone)]
pub struct OdbcCommandHandler {
    odbc_connection_string: String,
}

impl OdbcCommandHandler {

    pub fn handle(&self, cmd: OdbcCommand) -> Result<(), MyServerError> {
        let odbc_environment = Environment::new()?;
        match cmd {
            OdbcCommand::Query( odbc_query_command) => self.handle_query_command(odbc_environment, odbc_query_command)
        }
    }

    fn handle_query_command(&self, odbc_environment: Environment, cmd: OdbcQueryRequest) -> Result<(), MyServerError> {

        //let connection =

        let connection = odbc_environment.connect_with_connection_string(self.odbc_connection_string.as_str())?;
        let parameters = ();

        let cursor = connection
            .execute(cmd.query.as_str(), parameters)?
            .expect("failed to get cursor for query...");

        let schema = arrow_odbc::arrow_schema_from(&cursor)?;

        cmd.response_sender.send(OdbcQueryResponse{
            //ipc message + schema???
            schema
        });

        Ok(())
    }
}

pub struct MyServer {
    odbc_command_sender: Sender<OdbcCommand>,
}

impl MyServer {

    pub fn new(odbc_connection_string: String) -> Result<MyServer, MyServerError> {

        let (odbc_command_sender, mut odbc_command_receiver) = mpsc::channel::<OdbcCommand>(32);

        let handler = OdbcCommandHandler {
            odbc_connection_string,
        };

        let _: tokio::task::JoinHandle<Result<(), MyServerError>> = tokio::spawn(async move {
            while let Some(cmd) = odbc_command_receiver.recv().await {
                println!("handling cmd: {:?}", cmd);
                let result = handler.clone().handle(cmd);
                if let Err(e) = result {
                    println!("failed to process error: {:?}", e);
                }
            }
            Ok(())
        });

        Ok(MyServer {
            odbc_command_sender,
        })
    }

    async fn get_flight_info_statement(&self, q: CommandStatementQuery, flight_descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {

        let command_sender = self.odbc_command_sender.clone();
        let (response_sender, response_receiver) = oneshot::channel();

        self.odbc_command_sender.send(OdbcCommand::Query(OdbcQueryRequest{
            query: q.query,
            response_sender,
        }))
            .await
            .map_err(sender_error_to_status)?;

        let responseF = response_receiver.await;
        let response = responseF.expect("failed to get response...");

        let fiep = FlightEndpoint {
            ticket: None,
            location: vec![]
        };

        //let any: prost_types::Any = prost::Message::decode(&*flight_descriptor.cmd).map_err(decode_error_to_status)?;
        //prost::Message::encode()

        /*
                let fb = schema_to_fb(&schema);

        // read back fields
        let ipc = ipc::root_as_schema(fb.finished_data()).unwrap();
         */

        let arrow_schema = response.schema;

        //let fb = schema_to_fb(&response.schema);
       //prost::Message::encode()
        //let data_gen = ipc::writer::IpcDataGenerator::default();
        //let ed = data_gen.schema_to_bytes(&arrow_schema, &arrow::ipc::writer::IpcWriteOptions::default());
        //let ipc_schema =  prost_types::Any::pack(&arrow_schema)?;
        //let xx = ipc_schema.as_any().encode_to_vec();

       // ipc::writer::
        //flight_schema_as_flatbuffer

        //let data_gen = arrow::ipc::writer::IpcDataGenerator::default();
       let options = arrow::ipc::writer::IpcWriteOptions::default();
        //let msg = data_gen.schema_to_bytes(&arrow_schema, &options);

        let msg = ipc_message_from_arrow_schema(&arrow_schema, &options)
            .map_err(arrow_error_to_status)?;

        let fi = FlightInfo {
            schema: msg,
            flight_descriptor: Some(flight_descriptor),
            endpoint: vec![fiep],
            total_records: -1,
            total_bytes: -1
        };

        Ok(Response::new(fi))
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

fn flight_schema_as_encoded_data(
    arrow_schema: &Schema,
    options: &IpcWriteOptions,
) -> ipc::writer::EncodedData {
    let data_gen = ipc::writer::IpcDataGenerator::default();
    data_gen.schema_to_bytes(arrow_schema, options)
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

pub fn ipc_message_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> Result<Vec<u8>, ArrowError> {
    let message = SchemaAsIpc::new(schema, options).try_into().expect("failed blah...");
    let IpcMessage(vals) = message;
    Ok(vals)
}

pub fn sender_error_to_status<T>(error: tokio::sync::mpsc::error::SendError<T>) -> tonic::Status {
    Status::unknown("sender error")
}

#[tonic::async_trait]
impl FlightService for MyServer {

    type HandshakeStream = ReceiverStream<Result<HandshakeResponse, Status>>;

    async fn handshake(&self, request: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        todo!()
    }

    type ListFlightsStream = ReceiverStream<Result<FlightInfo, Status>>;

    async fn list_flights(&self, request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {

        let (mut tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {

            let fiep = FlightEndpoint {
                ticket: None,
                location: vec![]
            };

            let fi = FlightInfo {
                schema: vec![],
                flight_descriptor: None,
                endpoint: vec![fiep],
                total_records: -1,
                total_bytes: -1
            };

            tx.send(Ok(fi)).await.unwrap();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {

        let flight_descriptor = request.into_inner();
        let any: prost_types::Any = prost::Message::decode(&*flight_descriptor.cmd).map_err(decode_error_to_status)?;

        println!("any type_url is: {}", any.type_url);

        if any.is::<CommandStatementQuery>() {
            return self
                .get_flight_info_statement(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    flight_descriptor,
                )
                .await;
        }

        Err(Status::unimplemented(format!(
            "get_flight_info: The defined request is invalid: {:?}",
            String::from_utf8(any.encode_to_vec()).unwrap()
        )))
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        todo!()
    }

    type DoGetStream = ReceiverStream<Result<FlightData, Status>>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {

        /*
        let (mut tx, rx) = mpsc::channel(100);
        let cstr = self.odbc_connection_string.clone();


        //tokio::spawn(async move {

            let odbc_environment = Environment::new()
                .expect("failed to create odbc_env");
            //.map_err(odbc_api_err_to_status)?;

            let connection = odbc_environment
                .connect_with_connection_string(cstr.as_str())
                //.map_err(odbc_api_err_to_status)?;
                .expect("failed to connect");

            let parameters = ();

            let cursor = connection
                .execute("SELECT * FROM dataquality.checks LIMIT 10", parameters)
                .map_err(odbc_api_err_to_status)?
                .expect("SELECT statement must produce a cursor");

            let max_batch_size = 3;

            let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)
                //.map_err(arrow_odbc_err_to_status)?;
                .expect("failed to create odbc reader");

            for batch in arrow_record_batches {

                let flight_data = FlightData{
                    flight_descriptor: None,
                    data_header: vec![],
                    app_metadata: vec![],
                    data_body: vec![]
                };

               //tx.send(Ok(flight_data));//.await.expect("failed to send batch.."); //;;.await.unwrap();
                log::info!("sending a batch back to the client");
                tx.blocking_send(Ok(flight_data)).expect("failed to send batch..");
            }

            let result: Result<bool, Status> = Ok(true);
            result
        });

        Ok(Response::new(ReceiverStream::new(rx)))*/
        todo!();
    }

    type DoPutStream = ReceiverStream<Result<PutResult, Status>>;

    async fn do_put(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        todo!()
    }

    type DoExchangeStream = ReceiverStream<Result<FlightData, Status>>;

    async fn do_exchange(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        todo!()
    }

    type DoActionStream = ReceiverStream<Result<ActionResult, Status>>;

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        todo!()
    }

    type ListActionsStream = ReceiverStream<Result<ActionType, Status>>;

    async fn list_actions(&self, request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        todo!()
    }
}

fn odbc_api_err_to_status(err: odbc_api::Error) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", err))
}

fn arrow_odbc_err_to_status(err: arrow_odbc::Error) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", err))
}

fn decode_error_to_status(err: prost::DecodeError) -> tonic::Status {
    tonic::Status::invalid_argument(format!("{:?}", err))
}

fn arrow_error_to_status(err: arrow::error::ArrowError) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", err))
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_url() {
        assert_eq!(
            TicketStatementQuery::type_url(),
            "type.googleapis.com/arrow.flight.protocol.sql.TicketStatementQuery"
        );
        assert_eq!(
            CommandStatementQuery::type_url(),
            "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
        );
    }

    #[test]
    fn test_prost_any_pack_unpack() -> ArrowResult<()> {
        let query = CommandStatementQuery {
            query: "select 1".to_string(),
        };
        let any = prost_types::Any::pack(&query)?;
        assert!(any.is::<CommandStatementQuery>());
        let unpack_query: CommandStatementQuery = any.unpack()?.unwrap();
        assert_eq!(query, unpack_query);
        Ok(())
    }
}