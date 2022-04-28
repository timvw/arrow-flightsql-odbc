use std::env;
use tonic::transport::Server;
use arrow_flightsql_odbc::{MyServer, MyServerError};
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_server::FlightServiceServer;

#[tokio::main]
async fn main() -> Result<(), MyServerError> {

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    let odbc_connection_string = env::var("ODBC_CONNECTION_STRING")
        .expect("Failed to find ODBC_CONNECTION_STRING environment variable.");

    log::info!("odbc_connection_string: {}", odbc_connection_string);

    let server_address = env::var("SERVER_ADDRESS")
        .unwrap_or("0.0.0.0:52358".to_string());

    log::info!("binding to: {}", server_address);

    let addr = server_address.parse()?;
    let myserver = MyServer::new(odbc_connection_string)?;

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