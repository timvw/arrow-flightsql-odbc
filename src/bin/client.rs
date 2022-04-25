use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::{convert, MessageHeader};
use arrow_flightsql_odbc::arrow_flight_protocol::flight_service_client::FlightServiceClient;
use arrow_flightsql_odbc::arrow_flight_protocol::{FlightData, FlightDescriptor, FlightInfo};
use arrow_flightsql_odbc::arrow_flight_protocol::flight_descriptor::DescriptorType;
use arrow_flightsql_odbc::arrow_flight_protocol_sql::{CommandGetCatalogs, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetTables, CommandGetTableTypes, CommandStatementQuery};
use prost::Message;
use tonic::transport::Channel;
use arrow_flightsql_odbc::myserver::*;
use clap::{Args, Parser, Subcommand};
use tonic::Streaming;
use crate::Commands::{GetCatalogs, GetExportedKeys};

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

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Execute(ExecuteArgs),
    GetCatalogs(GetCatalogsArgs),
    GetTableTypes(GetTableTypesArgs),
    GetSchemas(GetSchemasArgs),
    GetTables(GetTablesArgs),
    GetExportedKeys(GetExportedKeysArgs),
    GetImportedKeys(GetImportedKeysArgs),
    GetPrimaryKeys(GetPrimaryKeysArgs),
}

#[derive(Args, Debug)]
struct Common {
    #[clap(long, default_value_t = String::from("localhost"))]
    hostname: String,
    #[clap(short, long, default_value_t = 52358, parse(try_from_str))]
    port: usize,
}

#[derive(Args, Debug)]
struct ExecuteArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    query: String
}

#[derive(Args, Debug)]
struct GetCatalogsArgs {
    #[clap(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct GetTableTypesArgs {
    #[clap(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct GetSchemasArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    schema: Option<String>,
}

#[derive(Args, Debug)]
struct GetTablesArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    schema: Option<String>,
    #[clap(short, long)]
    table: Option<String>,
}

#[derive(Args, Debug)]
struct GetExportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[derive(Args, Debug)]
struct GetImportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[derive(Args, Debug)]
struct GetPrimaryKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

async fn new_client(hostname: String, port: &usize) -> Result<FlightServiceClient<Channel>, ClientError> {
    let client_address = format!("http://{}:{}", hostname, port);
    FlightServiceClient::connect(client_address)
        .await
        .map_err(|e|ClientError::Tonic(format!("{}", e)) )
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {

    let cli = Cli::parse();

    match &cli.command {
        Commands::Execute (ExecuteArgs { common: Common{hostname, port}, query}) => {
            let client = new_client(hostname.to_string(), port).await?;
            execute(client,
                    query.to_string()
            ).await
        }
        Commands::GetCatalogs (GetCatalogsArgs { common: Common{hostname, port}}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_catalogs(client).await
        }
        Commands::GetTableTypes (GetTableTypesArgs { common: Common{hostname, port}}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_table_types(client).await
        }
        Commands::GetSchemas (GetSchemasArgs { common: Common{hostname, port}, catalog, schema}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_schemas(client,
                        catalog.as_deref().map(|x| x.to_string()),
                        schema.as_deref().map(|x| x.to_string())
            ).await
        }
        Commands::GetTables (GetTablesArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_tables(client,
                       catalog.as_deref().map(|x| x.to_string()),
                       schema.as_deref().map(|x|x.to_string()),
                       table.as_deref().map(|x|x.to_string())
            ).await
        }
        Commands::GetExportedKeys (GetExportedKeysArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_exported_keys(client,
                              catalog.as_deref().map(|x| x.to_string()),
                              schema.as_deref().map(|x|x.to_string()),
                              table.to_string()
            ).await
        }
        Commands::GetImportedKeys (GetImportedKeysArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_imported_keys(client,
                              catalog.as_deref().map(|x| x.to_string()),
                              schema.as_deref().map(|x|x.to_string()),
                              table.to_string()
            ).await
        }
        Commands::GetPrimaryKeys (GetPrimaryKeysArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_primary_keys(client,
                             catalog.as_deref().map(|x| x.to_string()),
                             schema.as_deref().map(|x|x.to_string()),
                             table.to_string()
            ).await
        }
    }
}

async fn execute(client: FlightServiceClient<Channel>, query: String) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandStatementQuery { query })
        .await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_exported_keys(client: FlightServiceClient<Channel>, catalog: Option<String>, schema: Option<String>, table: String) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetExportedKeys {
        catalog,
        db_schema: schema,
        table
    }).await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_imported_keys(client: FlightServiceClient<Channel>, catalog: Option<String>, schema: Option<String>, table: String) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetImportedKeys {
        catalog,
        db_schema: schema,
        table
    }).await?;

    print_flight_info_results(client, fi)
        .await
}

async fn get_primary_keys(client: FlightServiceClient<Channel>, catalog: Option<String>, schema: Option<String>, table: String) -> Result<(), ClientError> {

    let fi = get_flight_descriptor_for_command(client.clone(), &CommandGetPrimaryKeys {
        catalog,
        db_schema: schema,
        table
    }).await?;

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