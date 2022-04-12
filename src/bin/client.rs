use futures::StreamExt;
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_client::FlightServiceClient;
use arrow_flightsql_odbc::arrow_flight_protocol::Empty;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;
    let actionsResponse = client.list_actions(Empty {}).await?;
    let mut actions = actionsResponse.into_inner();

    while let Some(at) = actions.next().await {
        let at = at?;
        println!("action type: {} desc: {}", at.r#type, at.description);
    }

    Ok(())
}