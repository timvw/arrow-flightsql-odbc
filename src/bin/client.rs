use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_client::FlightServiceClient;
use arrow_flightsql_odbc::arrow_flight_protocol::{Criteria, FlightDescriptor};
use arrow_flightsql_odbc::arrow_flight_protocol::flight_descriptor::DescriptorType;
use arrow_flightsql_odbc::arrow_flight_protocol_sql::CommandStatementQuery;
use prost::Message;
use arrow_flightsql_odbc::myserver::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    let mut flight_infos = client
        .list_flights(Criteria{ expression: vec![] })
        .await?
        .into_inner();

    let cmd = CommandStatementQuery { query: "SELECT * FROM TEST.T1".to_string() };
    //let cmdBytes = cmd.encode_to_vec();
    //prost::Message::encode(cmd);
    let any = prost_types::Any::pack(&cmd)?;

    let fi = client
        .get_flight_info(FlightDescriptor{
            r#type: DescriptorType::Cmd as i32,
            cmd: any.encode_to_vec(),
            path: vec![]
        })
        .await?
        .into_inner();

    println!("total bytes: {}, total records: {}", fi.total_bytes, fi.total_records);

    while let Some(fi) = flight_infos.message().await? {
        println!("total bytes: {}, total records: {}", fi.total_bytes, fi.total_records);
    }

    /*
    let mut action_types = client
        .list_actions(Empty {})
        .await?
        .into_inner();

    while let Some(action_type) = action_types.message().await? {
        println!("action type: {} desc: {}", action_type.r#type, action_type.description);
    }*/

    Ok(())
}