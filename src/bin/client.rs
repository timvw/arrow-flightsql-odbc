use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::{convert, MessageHeader};
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_client::FlightServiceClient;
use arrow_flightsql_odbc::arrow_flight_protocol::{FlightData, FlightDescriptor, FlightInfo};
use arrow_flightsql_odbc::arrow_flight_protocol::flight_descriptor::DescriptorType;
use arrow_flightsql_odbc::arrow_flight_protocol_sql::{CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, CommandGetTableTypes, CommandStatementQuery};
use prost::Message;
use tonic::transport::Channel;
use arrow_flightsql_odbc::myserver::*;
use clap::{arg, Command};
use tonic::Streaming;

#[derive(Debug)]
pub enum ClientError {
    Logic(String),
    ArrowError(String),
    Tonic(String),
    DataError(String),
}

impl From<arrow::error::ArrowError> for ClientError {
    fn from(error: arrow::error::ArrowError) -> Self {
        ClientError::ArrowError(error.to_string())
    }
}

impl From<tonic::Status> for ClientError {
    fn from(status: tonic::Status) -> Self { ClientError::Tonic(format!("{}", status)) }
}

impl From<tonic::transport::Error> for ClientError {
    fn from(error: tonic::transport::Error) -> Self { ClientError::Tonic(format!("{}", error)) }
}

fn cli() -> Command<'static> {
    Command::new("FlightSqlClientDemoApp")
        .about("A Flight Sql client CLI")
        .arg(arg!([HOST])
            .help("The host where the Flight Sql server is running")
            .default_value("localhost"))
        .arg(arg!([PORT])
            .help( "The port where the Flight Sql server is running")
            //.default_value("50051")
            .default_value("52358")
            .validator(|s| s.parse::<usize>()))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("Execute")
                .about("Execute a SQL query")
                .arg(arg!(<QUERY> "The query to execute"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("GetCatalogs")
                .about("Get catalogs")
        )
        .subcommand(
            Command::new("GetTableTypes")
                .about("Get table types")
        )
        .subcommand(
            Command::new("GetSchemas")
                .about("Get schemas")
                .arg(arg!([catalog])
                         .help("The catalog to use"))
                .arg(arg!([schema])
                         .help("Specifies a filter pattern for schemas to search for. When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search."))
        )
        .subcommand(
            Command::new("GetTables")
                .about("Get tables")
                .arg(arg!([catalog])
                    .help("The catalog to use"))
                .arg(arg!([schema])
                    .help("Specifies a filter pattern for schemas to search for. When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search."))
                .arg(arg!([table])
                    .help("The table to use"))
        )
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {

    let matches = cli().get_matches();

    let host = matches
        .value_of("HOST")
        .expect("'HOST' is required");

    let port: usize = matches
        .value_of_t("PORT")
        .expect("'PORT' is required");

    let client_address = format!("http://{}:{}", host, port);

    let client = FlightServiceClient::connect(client_address)
        .await?;

    match matches.subcommand() {
        Some(("Execute", sub_matches)) => {
            let query = sub_matches.value_of("QUERY").expect("'QUERY' is required").to_string();
            execute(client, query).await
        }
        Some(("GetCatalogs", _)) => {
            get_catalogs(client).await
        }
        Some(("GetTableTypes", _)) => {
            get_table_types(client).await
        }
        Some(("GetSchemas", sub_matches)) => {
            let catalog = sub_matches.value_of("catalog").map(|x| x.to_string());
            let schema = sub_matches.value_of("schema").map(|x| x.to_string());
            get_schemas(client, catalog, schema).await
        }
        Some(("GetTables", sub_matches)) => {
            let catalog = sub_matches.value_of("catalog").map(|x| x.to_string());
            let schema = sub_matches.value_of("schema").map(|x| x.to_string());
            let table = sub_matches.value_of("table").map(|x| x.to_string());
            get_tables(client, catalog, schema, table).await
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}

async fn execute(client: FlightServiceClient<Channel>, query: String) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandStatementQuery { query })
        .await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_tables(client: FlightServiceClient<Channel>, catalog: Option<String>, schema: Option<String>, table: Option<String>) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetTables {
        catalog: catalog,
        db_schema_filter_pattern: schema,
        table_name_filter_pattern: table,
        table_types: vec![],
        include_schema: false,
    }).await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_catalogs(client: FlightServiceClient<Channel>) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetCatalogs { })
        .await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_schemas(client: FlightServiceClient<Channel>, catalog: Option<String>, schema: Option<String>) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetDbSchemas {
        catalog: catalog,
        db_schema_filter_pattern: schema,
    }).await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_table_types(client: FlightServiceClient<Channel>) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetTableTypes { })
        .await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_flight_descriptor_for_command<M: ProstMessageExt>(mut client: FlightServiceClient<Channel>, message: &M) -> Result<FlightInfo, ClientError> {

    let any = prost_types::Any::pack(message)?;

    let fi = client
        .get_flight_info(FlightDescriptor{
            r#type: DescriptorType::Cmd as i32,
            cmd: any.encode_to_vec(),
            path: vec![]
        })
        .await?
        .into_inner();

    Ok(fi)
}

async fn print_flight_info_results(mut client: FlightServiceClient<Channel>, fi: FlightInfo) -> Result<(), ClientError> {

    let first_endpoint = fi.endpoint.first()
        .ok_or(ClientError::Logic("Failed to get first endpoint".to_string()))?;

    let first_ticket = first_endpoint.ticket.clone()
        .ok_or(ClientError::Logic("Failed to get first ticket".to_string()))?;

    let mut flight_data_stream = client
        .do_get(first_ticket)
        .await?
        .into_inner();

    print_flight_data_stream(&mut flight_data_stream)
        .await
}

async fn print_flight_data_stream(flight_data_stream: &mut Streaming<FlightData>) -> Result<(), ClientError> {

    let first_message = flight_data_stream.message().await?.expect("failed to get schema message...");
    let first_ipc_message = arrow::ipc::root_as_message(&first_message.data_header[..])
        .map_err(|err| { ArrowError::ParseError(format!("Unable to get root as message: {:?}", err)) })?;
    let ipc_schema = first_ipc_message
        .header_as_schema()
        .expect("failed to get schema from first message");
    let arrow_schema =convert::fb_to_schema(ipc_schema);
    let arrow_schema_ref = SchemaRef::new(arrow_schema);

    while let Some(flight_data) = flight_data_stream.message().await? {
        let ipc_message = arrow::ipc::root_as_message(&flight_data.data_header[..])
            .map_err(|err| { ArrowError::ParseError(format!("Unable to get root as message: {:?}", err)) })?;

        if ipc_message.header_type() == MessageHeader::RecordBatch {
            let ipc_record_batch = ipc_message
                .header_as_record_batch()
                .ok_or(ClientError::Logic("Unable to convert flight data header to a record batch".to_string()))?;

            let dictionaries_by_field = &[];
            let record_batch = arrow::ipc::reader::read_record_batch(&flight_data.data_body, ipc_record_batch, arrow_schema_ref.clone(), dictionaries_by_field, None)?;

            arrow::util::pretty::print_batches(&[record_batch])?;
        }
    }

    Ok(())
}