use std::env;
use arrow_odbc::odbc_api::Environment;
use arrow_odbc::{arrow_schema_from, OdbcReader};
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    println!("test app");

    let odbc_connection_string = env::var("ODBC_CONNECTION_STRING")
        .expect("Failed to find ODBC_CONNECTION_STRING environment variable.");

    let odbc_environment = Environment::new()
        .expect("failed to create odbc_env");
    //.map_err(odbc_api_err_to_status)?;

    let connection = odbc_environment
        .connect_with_connection_string(odbc_connection_string.as_str())
        //.map_err(odbc_api_err_to_status)?;
        .expect("failed to connect");

    let parameters = ();

    let cursor = connection
        .execute("SELECT * FROM dataquality.checks LIMIT 1", parameters)
        //.map_err(odbc_api_err_to_status)?
        .expect("SELECT statement must produce a cursor")
        .expect("really need smth..");

    let max_batch_size = 3;

    //let schema = Arc::new(arrow_schema_from(&cursor)?);
    //pub fn arrow_schema_from(resut_set_metadata: &impl ResultSetMetadata) -> Result<Schema, Error> {

    let schema = arrow_schema_from(&cursor).expect("fialed to get schema");
    println!("schema: {:?}", schema);

    /*
    let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)
        //.map_err(arrow_odbc_err_to_status)?;
        .expect("failed to create odbc reader");

    for batchr in arrow_record_batches {
        let batch = batchr.expect("failed to fetch batch");
        println!("columns: {:?}", batch.columns());
    }*/

    Ok(())
}