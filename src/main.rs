use arrow_odbc::odbc_api::{Environment};
use std::env;

fn main() {

    let connection_string = env::var("DSN")
        .expect("Did not find DSN environment variable");

    println!("dsn: {}", connection_string);

    // If you do not do anything fancy it is recommended to have only one Environment in the
    // entire process.
    let environment = Environment::new()
        .expect("failed to create new odbc environment");

    // Connect using a DSN. Alternatively we could have used a connection string
    let mut connection = environment
        .connect_with_connection_string(&connection_string)
        .expect("failed to connect to server");

}
