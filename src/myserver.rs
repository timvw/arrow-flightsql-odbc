use tonic::{Request, Response, Status, Streaming};
use crate::arrow_flight_protocol::flight_service_server::FlightService;
use crate::arrow_flight_protocol::{Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaResult, Ticket, PutResult, Result as ActionResult, ActionType, FlightEndpoint};
use arrow::datatypes::Schema;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use crate::arrow_flight_protocol_sql::*;
use prost::Message;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc;
use arrow_odbc::{odbc_api, OdbcReader};
use arrow_odbc::odbc_api::{CursorImpl, Environment};
use tokio::sync::mpsc::Sender;
use arrow::ipc::writer::IpcWriteOptions;
use core::ops::Deref;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::EncodedData;
use arrow_odbc::odbc_api::handles::StatementImpl;
use tokio::task;
use crate::error::MyServerError;

#[derive(Debug)]
pub enum OdbcCommand {
    GetCommandSchema(GetCommandSchemaRequest),
    GetCommandData(GetCommandDataRequest),
}

#[derive(Debug)]
pub struct GetCommandSchemaRequest {
    command: FlightSqlCommand,
    response_sender: oneshot::Sender<GetSchemaResponse>,
}

#[derive(Debug)]
pub struct GetCommandDataRequest {
    command: FlightSqlCommand,
    response_sender: tokio::sync::mpsc::Sender<Result<FlightData, Status>>,
}

#[derive(Debug)]
pub struct GetSchemaResponse {
    ticket: Ticket,
    schema: Schema,
}

//#[derive(Debug)]
pub struct OdbcCommandHandler {
    odbc_connection_string: String,
    odbc_environment: Environment,
}

#[derive(Debug, Clone)]
pub enum FlightSqlCommand {
    StatementQuery(CommandStatementQuery),
    GetTables(CommandGetTables),
}

impl FlightSqlCommand {

    fn try_parse_bytes<B: bytes::Buf>(buf: B) -> Result<FlightSqlCommand, MyServerError> {
        let any: prost_types::Any = prost::Message::decode(buf)
            .map_err(decode_error_to_status)?;

        match any {
            _ if any.is::<CommandStatementQuery>() => {
                let command = any.unpack()?
                    .expect("unreachable");
                Ok(FlightSqlCommand::StatementQuery(command))
            },
            _ if any.is::<CommandGetTables>() => {
                let command = any.unpack()
                    .map_err(arrow_error_to_status)?
                    .expect("unreachable");
                Ok(FlightSqlCommand::GetTables(command))
            },
            _ => Err(MyServerError::NotImplementedYet(format!("still need to implement support for {}", any.type_url))),
        }
    }

    fn try_parse_ticket(ticket: Ticket) -> Result<FlightSqlCommand, MyServerError> {
        FlightSqlCommand::try_parse_bytes(&*ticket.ticket)
    }

    fn try_parse_flight_descriptor(flight_descriptor: FlightDescriptor) -> Result<FlightSqlCommand, MyServerError> {
        FlightSqlCommand::try_parse_bytes(&*flight_descriptor.cmd)
    }

    fn to_ticket(&self) -> Ticket {
        let ticket = match self {
            FlightSqlCommand::StatementQuery(cmd) => cmd.as_any().encode_to_vec(),
            FlightSqlCommand::GetTables(cmd) => cmd.as_any().encode_to_vec(),
        };
        Ticket {
            ticket,
        }
    }
}

impl OdbcCommandHandler {

    pub fn handle(&mut self, cmd: OdbcCommand) -> Result<(), MyServerError> {
        match cmd {
            OdbcCommand::GetCommandSchema(x) => self.handle_get_command_schema(x),
            OdbcCommand::GetCommandData(x) => self.handle_get_command_data(x),
        }
    }

    fn get_connection(&self) -> Result<odbc_api::Connection<'_>, MyServerError> {
        self.odbc_environment.connect_with_connection_string(self.odbc_connection_string.as_str())
            .map_err(|e| MyServerError::OdbcApiError(e))
    }

    fn get_result_cursor<'s>(&self, connection: &'s odbc_api::Connection<'s>, command: FlightSqlCommand) -> Result<CursorImpl<StatementImpl<'s>>, MyServerError> {
        match command {
            FlightSqlCommand::StatementQuery(x) => self.get_statement_query(&connection, x),
            FlightSqlCommand::GetTables(x) => self.get_tables_query(&connection, x),
        }
    }

    fn handle_get_command_schema(&mut self, req: GetCommandSchemaRequest)-> Result<(), MyServerError> {
        let connection = self.get_connection()?;
        let cursor = self.get_result_cursor(&connection, req.command.clone())?;
        let ticket = req.command.to_ticket();
        self.send_schema_from_cursor(req.response_sender, cursor, ticket)
    }

    fn handle_get_command_data(&mut self, req: GetCommandDataRequest)-> Result<(), MyServerError> {
        let connection = self.get_connection()?;
        let cursor = self.get_result_cursor(&connection, req.command.clone())?;
        self.send_flight_data_from_cursor(req.response_sender, cursor)
    }

    fn get_statement_query<'s>(&self, connection: &'s odbc_api::Connection<'s>, cmd: CommandStatementQuery) -> Result<CursorImpl<StatementImpl<'s>>, MyServerError> {
        let parameters = ();
        let cursor = connection
            .execute(cmd.query.as_str(), parameters)?
            .expect("failed to get cursor for query...");
        Ok(cursor)
    }

    fn get_tables_query<'s>(&self, connection: &'s odbc_api::Connection<'s>, cmd: CommandGetTables) -> Result<CursorImpl<StatementImpl<'s>>, MyServerError> {
        let cursor = connection.tables(
            cmd.catalog.unwrap_or("".to_string()).as_str(),
            cmd.db_schema_filter_pattern.unwrap_or("".to_string()).as_str(),
            cmd.table_name_filter_pattern.unwrap_or("".to_string()).as_str(),
            "")?;
        Ok(cursor)
    }

    fn send_schema_from_cursor<'s>(&self, response_sender: oneshot::Sender<GetSchemaResponse>, cursor: CursorImpl<StatementImpl<'s>>, ticket: Ticket) -> Result<(), MyServerError> {
        let schema = arrow_odbc::arrow_schema_from(&cursor)?;

        response_sender.send(GetSchemaResponse {
            ticket,
            schema
        }).map_err(|_| MyServerError::SendError("failed to response...".to_string()))
    }

    fn send_flight_data_from_cursor<'s>(&self, response_sender: tokio::sync::mpsc::Sender<Result<FlightData, Status>>, cursor: CursorImpl<StatementImpl<'s>>) -> Result<(), MyServerError> {
        let arrow_record_batches = OdbcReader::new(cursor, 100)
            //.map_err(arrow_odbc_err_to_status)?;
            .expect("failed to create odbc reader");

        for batchr in arrow_record_batches {
            let batch = batchr.expect("failed to fetch batch");

            let (dicts, batch) = flight_data_from_arrow_batch(&batch, &IpcWriteOptions::default());

            let rsp = response_sender.clone();
            task::spawn_blocking(move || {
                for dict in dicts {
                    let result = rsp.blocking_send(Ok(dict));
                    if let Err(_) = result {
                        log::error!("failed to send dict...");
                    }
                }
                let result = rsp.blocking_send(Ok(batch));
                if let Err(_) = result {
                    log::error!("failed to send data...");
                }
            });
        }

        Ok(())
    }
}

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and a `FlightData` representing the bytes of the batch's values
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let data_gen = ipc::writer::IpcDataGenerator::default();
    let mut dictionary_tracker = ipc::writer::DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, options)
        .expect("DictionaryTracker configured above to not error.rs on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();

    (flight_dictionaries, flight_batch)
}

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message,
            data_body: data.arrow_data,
            ..Default::default()
        }
    }
}

pub struct MyServer {
    odbc_command_sender: Sender<OdbcCommand>,
}

impl MyServer {

    pub fn new(odbc_connection_string: String) -> Result<MyServer, MyServerError> {

        let (odbc_command_sender, mut odbc_command_receiver) = mpsc::channel::<OdbcCommand>(32);

        let _: tokio::task::JoinHandle<Result<(), MyServerError>> = tokio::spawn(async move {

            let odbc_environment = Environment::new()?;

            let mut handler = OdbcCommandHandler {
                odbc_connection_string,
                odbc_environment,
            };

            while let Some(cmd) = odbc_command_receiver.recv().await {
                log::info!("handling cmd: {:?}", cmd);
                let result = handler.handle(cmd);
                if let Err(e) = result {
                    log::error!("failed to process error.rs: {:?}", e);
                }
            }

            Ok(())
        });

        Ok(MyServer {
            odbc_command_sender,
        })
    }

    fn make_flight_info(&self, flight_descriptor: FlightDescriptor, arrow_schema: Schema, ticket: Ticket) -> Result<Response<FlightInfo>, Status> {

        let fiep = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![]
        };

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let ipc_schema = ipc_message_from_arrow_schema(&arrow_schema, &options)
            .map_err(arrow_error_to_status)?;

        Ok(Response::new(FlightInfo {
            schema: ipc_schema,
            flight_descriptor: Some(flight_descriptor),
            endpoint: vec![fiep],
            total_records: -1,
            total_bytes: -1
        }))
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

pub fn myserver_error_to_status(_: MyServerError) -> tonic::Status {
    Status::unknown("myserver error")
}

pub fn sender_error_to_status<T>(_: tokio::sync::mpsc::error::SendError<T>) -> tonic::Status {
    Status::unknown("sender error.rs")
}

pub fn receiver_error_to_status(_: tokio::sync::oneshot::error::RecvError) -> tonic::Status {
    Status::unknown("receiver error.rs")
}

pub fn utf8_err_to_status(_: core::str::Utf8Error) -> tonic::Status {
    Status::unknown("utf8 error.rs")
}

#[tonic::async_trait]
impl FlightService for MyServer {

    type HandshakeStream = ReceiverStream<Result<HandshakeResponse, Status>>;

    async fn handshake(&self, _: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        todo!()
    }

    type ListFlightsStream = ReceiverStream<Result<FlightInfo, Status>>;

    async fn list_flights(&self, _: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        todo!()
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {

        let flight_descriptor = request.into_inner();

        let command = FlightSqlCommand::try_parse_flight_descriptor(flight_descriptor.clone())
            .map_err(myserver_error_to_status)?;

        let (response_sender, response_receiver) = oneshot::channel();

        self.odbc_command_sender
            .send(OdbcCommand::GetCommandSchema(GetCommandSchemaRequest {
                command,
                response_sender,
            }))
            .await
            .map_err(sender_error_to_status)?;

        let response = response_receiver
            .await
            .map_err(receiver_error_to_status)?;

        self.make_flight_info(flight_descriptor, response.schema, response.ticket)
    }

    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        todo!()
    }

    type DoGetStream = ReceiverStream<Result<FlightData, Status>>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {

        let ticket = request.into_inner();

        let command = FlightSqlCommand::try_parse_ticket(ticket.clone())
            .map_err(myserver_error_to_status)?;

        let (response_sender, response_receiver) = mpsc::channel(100);

        self.odbc_command_sender
            .send(OdbcCommand::GetCommandData(GetCommandDataRequest {
                command,
                response_sender,
            }))
            .await
            .map_err(sender_error_to_status)?;

        Ok(Response::new(ReceiverStream::new(response_receiver)))
    }

    type DoPutStream = ReceiverStream<Result<PutResult, Status>>;

    async fn do_put(&self, _: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        todo!()
    }

    type DoExchangeStream = ReceiverStream<Result<FlightData, Status>>;

    async fn do_exchange(&self, _: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        todo!()
    }

    type DoActionStream = ReceiverStream<Result<ActionResult, Status>>;

    async fn do_action(&self, _: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        todo!()
    }

    type ListActionsStream = ReceiverStream<Result<ActionType, Status>>;

    async fn list_actions(&self, _: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        todo!()
    }
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