use tonic::transport::Server;
use arrow_flightsql_odbc::myserver::MyServer;
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_server::FlightServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    let addr = "0.0.0.0:50051".parse()?;
    let myserver = MyServer::new();

    let reflection_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(arrow_flightsql_odbc::FLIGHT_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(arrow_flightsql_odbc::FLIGHT_SQL_DESCRIPTOR_SET)
        .build()?;

    Server::builder()
        .add_service(FlightServiceServer::new(myserver))
        .add_service(reflection_server)
        .serve(addr)
        .await?;

    Ok(())
}