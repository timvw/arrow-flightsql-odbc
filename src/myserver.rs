use tonic::{Request, Response, Status, Streaming};
use crate::arrow_flight_protocol::flight_service_server::FlightService;
use crate::arrow_flight_protocol::{Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaResult, Ticket, PutResult, Result as ActionResult, ActionType, FlightEndpoint};
use futures_core::Stream;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::arrow_flight_protocol_sql::*;
use prost::Message;
use prost_types::Any;
use arrow::error::{ArrowError, Result as ArrowResult};

#[derive(Debug)]
pub struct MyServer {}

impl MyServer {
    pub fn new() -> MyServer {
        MyServer {
        }
    }

    async fn get_flight_info_statement(&self, q: CommandStatementQuery, flight_descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {

        let fiep = FlightEndpoint {
            ticket: None,
            location: vec![]
        };

        let fi = FlightInfo {
            schema: vec![],
            flight_descriptor: Some(flight_descriptor),
            endpoint: vec![fiep],
            total_records: -1,
            total_bytes: -1
        };

        Ok(Response::new(fi))
    }
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
        todo!()
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