use tonic::{Request, Response, Status, Streaming};
use crate::arrow_flight_protocol::flight_service_server::FlightService;
use crate::arrow_flight_protocol::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, Result as ActionResult, SchemaResult, Ticket};
use arrow::datatypes::Schema;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc;
use arrow_odbc::odbc_api::Environment;
use tokio::sync::mpsc::Sender;
use arrow::ipc::writer::IpcWriteOptions;
use core::ops::Deref;
use crate::error;
use crate::error::MyServerError;
use crate::flight_sql_command::FlightSqlCommand;
use crate::odbc_command_handler::{GetCommandDataRequest, GetCommandSchemaRequest, OdbcCommand, OdbcCommandHandler};

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
            .map_err(error::arrow_error_to_status)?;

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

fn myserver_error_to_status(_: MyServerError) -> tonic::Status {
    Status::unknown("myserver error")
}

fn sender_error_to_status<T>(_: tokio::sync::mpsc::error::SendError<T>) -> tonic::Status {
    Status::unknown("sender error.rs")
}

fn receiver_error_to_status(_: tokio::sync::oneshot::error::RecvError) -> tonic::Status {
    Status::unknown("receiver error")
}