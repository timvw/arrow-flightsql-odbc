use crate::arrow_flight_protocol::{FlightData, Ticket};
use crate::arrow_flight_protocol_sql::{CommandGetTables, CommandStatementQuery};
use crate::error::MyServerError;
use crate::flight_sql_command::FlightSqlCommand;
use arrow::datatypes::Schema;
use arrow::ipc::writer::EncodedData;
use arrow_odbc::odbc_api::handles::StatementImpl;
use arrow_odbc::odbc_api::{Connection, ConnectionOptions, CursorImpl, Environment};
use arrow_odbc::OdbcReaderBuilder;
use tokio::sync::oneshot;
use tokio::task;
use tonic::Status;
use crate::util::batches_to_flight_data;

#[derive(Debug)]
pub enum OdbcCommand {
    GetCommandSchema(GetCommandSchemaRequest),
    GetCommandData(GetCommandDataRequest),
}

#[derive(Debug)]
pub struct GetCommandSchemaRequest {
    pub command: FlightSqlCommand,
    pub response_sender: oneshot::Sender<GetSchemaResponse>,
}

#[derive(Debug)]
pub struct GetCommandDataRequest {
    pub command: FlightSqlCommand,
    pub response_sender: tokio::sync::mpsc::Sender<Result<FlightData, Status>>,
}

#[derive(Debug)]
pub struct GetSchemaResponse {
    pub ticket: Ticket,
    pub schema: Schema,
}

//#[derive(Debug)]
pub struct OdbcCommandHandler {
    pub odbc_connection_string: String,
    pub odbc_environment: Environment,
}

impl OdbcCommandHandler {
    pub fn handle(&mut self, cmd: OdbcCommand) -> Result<(), MyServerError> {
        match cmd {
            OdbcCommand::GetCommandSchema(x) => self.handle_get_command_schema(x),
            OdbcCommand::GetCommandData(x) => self.handle_get_command_data(x),
        }
    }

    fn get_connection(&self) -> Result<Connection<'_>, MyServerError> {
        self.odbc_environment
            .connect_with_connection_string(self.odbc_connection_string.as_str(), ConnectionOptions::default())
            .map_err(|e| MyServerError::OdbcApiError(e))
    }

    fn get_result_cursor<'s>(
        &self,
        connection: &'s Connection<'s>,
        command: FlightSqlCommand,
    ) -> Result<CursorImpl<StatementImpl<'s>>, MyServerError> {
        match command {
            FlightSqlCommand::StatementQuery(x) => self.get_statement_query(&connection, x),
            FlightSqlCommand::GetTables(x) => self.get_tables_query(&connection, x),
        }
    }

    fn handle_get_command_schema(
        &mut self,
        req: GetCommandSchemaRequest,
    ) -> Result<(), MyServerError> {
        let connection = self.get_connection()?;
        let mut cursor = self.get_result_cursor(&connection, req.command.clone())?;
        let ticket = req.command.to_ticket();
        self.send_schema_from_cursor(req.response_sender, &mut cursor, ticket)
    }

    fn handle_get_command_data(&mut self, req: GetCommandDataRequest) -> Result<(), MyServerError> {
        let connection = self.get_connection()?;
        let mut cursor = self.get_result_cursor(&connection, req.command.clone())?;
        let schema = arrow_odbc::arrow_schema_from(&mut cursor)?;
        let cursor1 = cursor;
        self.send_flight_data_from_cursor(&schema, req.response_sender, cursor1)
    }

    fn get_statement_query<'s>(
        &self,
        connection: &'s Connection<'s>,
        cmd: CommandStatementQuery,
    ) -> Result<CursorImpl<StatementImpl<'s>>, MyServerError> {
        let parameters = ();
        let cursor = connection
            .execute(cmd.query.as_str(), parameters)?
            .expect("failed to get cursor for query...");
        Ok(cursor)
    }

    fn get_tables_query<'s>(
        &self,
        connection: &'s Connection<'s>,
        cmd: CommandGetTables,
    ) -> Result<CursorImpl<StatementImpl<'s>>, MyServerError> {
        let cursor = connection.tables(
            cmd.catalog.unwrap_or("".to_string()).as_str(),
            cmd.db_schema_filter_pattern
                .unwrap_or("".to_string())
                .as_str(),
            cmd.table_name_filter_pattern
                .unwrap_or("".to_string())
                .as_str(),
            "",
        )?;
        Ok(cursor)
    }

    fn send_schema_from_cursor<'s>(
        &self,
        response_sender: oneshot::Sender<GetSchemaResponse>,
        cursor: &mut CursorImpl<StatementImpl<'s>>,
        ticket: Ticket,
    ) -> Result<(), MyServerError> {
        let schema = arrow_odbc::arrow_schema_from(cursor)?;

        response_sender
            .send(GetSchemaResponse { ticket, schema })
            .map_err(|_| MyServerError::SendError("failed to response...".to_string()))
    }

    fn send_flight_data_from_cursor<'s>(
        &self,
        schema: &Schema,
        response_sender: tokio::sync::mpsc::Sender<Result<FlightData, Status>>,
        cursor: CursorImpl<StatementImpl<'s>>,
    ) -> Result<(), MyServerError> {
        let arrow_record_batches = OdbcReaderBuilder::new().build(cursor)?;
            //.map_err(arrow_odbc_err_to_status)?;
            //.expect("failed to create odbc reader");

        let mut batchvec = vec![];
        for batchr in arrow_record_batches {
            let batch = batchr.expect("failed to fetch batch");
            batchvec.push(batch);
        }

        log::info!("batchvec size : {}", batchvec.len());

        match batches_to_flight_data(&schema, batchvec) {
            Ok(flight_data_vec) => {
                log::info!("flight_data_vec size : {}", flight_data_vec.len());
                for flight_data in flight_data_vec {
                    let rsp = response_sender.clone();
                        task::spawn_blocking(move || {
                                let result = rsp.blocking_send(Ok(flight_data));
                                if let Err(_) = result {
                                    log::error!("failed to send flight_data...");
                                }
                        });
                }
            }
            Err(arrow_error) => {
                // Handle the error (e.g., log it)
                log::error!("Error converting batches to FlightData: {}", arrow_error);
            }
        }

        Ok(())
    }
}

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message,
            data_body: data.arrow_data,
            ..Default::default()
        }
    }
}
