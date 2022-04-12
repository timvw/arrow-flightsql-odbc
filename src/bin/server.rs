use tonic::transport::Server;
use arrow_flightsql_odbc::myserver::MyServer;
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_server::FlightServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    let addr = "0.0.0.0:50051".parse()?;
    let myserver = MyServer::new();

    Server::builder()
        .add_service(FlightServiceServer::new(myserver))
        .serve(addr)
        .await?;

    Ok(())
}