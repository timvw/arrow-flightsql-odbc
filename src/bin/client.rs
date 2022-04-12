use futures::StreamExt;
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_client::FlightServiceClient;
use arrow_flightsql_odbc::arrow_flight_protocol::Empty;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    let mut action_types = client
        .list_actions(Empty {})
        .await?
        .into_inner();

    while let Some(action_type) = action_types.message().await? {
        println!("action type: {} desc: {}", action_type.r#type, action_type.description);
    }

    Ok(())
}