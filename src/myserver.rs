use tonic::{Request, Response, Status, Streaming};
use crate::arrow_flight_protocol::flight_service_server::FlightService;
use crate::arrow_flight_protocol::{Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaResult, Ticket, PutResult, Result as ActionResult, ActionType};
use futures_core::Stream;
use std::pin::Pin;

#[derive(Debug)]
pub struct MyServer {}

impl MyServer {
    pub fn new() -> MyServer {
        MyServer {
        }
    }
}

#[tonic::async_trait]
impl FlightService for MyServer {

    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;

    async fn handshake(&self, request: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        todo!()
    }

    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;

    async fn list_flights(&self, request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        todo!()
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        todo!()
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        todo!()
    }

    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        todo!()
    }

    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;

    async fn do_put(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        todo!()
    }

    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_exchange(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        todo!()
    }

    type DoActionStream = Pin<Box<dyn Stream<Item = Result<ActionResult, Status>> + Send + Sync + 'static>>;

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        todo!()
    }

    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn list_actions(&self, request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        todo!()
    }
}