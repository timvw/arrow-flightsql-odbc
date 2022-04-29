use crate::arrow_flight_protocol::{FlightData, Ticket};
use crate::arrow_flight_protocol_sql::{CommandGetTables, CommandStatementQuery};
use crate::error::MyServerError;
use crate::flight_sql_command::FlightSqlCommand;
use arrow::datatypes::Schema;
use arrow::ipc;
use arrow::ipc::writer::{EncodedData, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use arrow_odbc::odbc_api::handles::StatementImpl;
use arrow_odbc::odbc_api::{Connection, CursorImpl, Environment};
use arrow_odbc::OdbcReader;
use tokio::sync::oneshot;
use tokio::task;
use tonic::Status;

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
            .connect_with_connection_string(self.odbc_connection_string.as_str())
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
        let cursor = self.get_result_cursor(&connection, req.command.clone())?;
        let ticket = req.command.to_ticket();
        self.send_schema_from_cursor(req.response_sender, cursor, ticket)
    }

    fn handle_get_command_data(&mut self, req: GetCommandDataRequest) -> Result<(), MyServerError> {
        let connection = self.get_connection()?;
        let cursor = self.get_result_cursor(&connection, req.command.clone())?;
        self.send_flight_data_from_cursor(req.response_sender, cursor)
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
        cursor: CursorImpl<StatementImpl<'s>>,
        ticket: Ticket,
    ) -> Result<(), MyServerError> {
        let schema = arrow_odbc::arrow_schema_from(&cursor)?;

        response_sender
            .send(GetSchemaResponse { ticket, schema })
            .map_err(|_| MyServerError::SendError("failed to response...".to_string()))
    }

    fn send_flight_data_from_cursor<'s>(
        &self,
        response_sender: tokio::sync::mpsc::Sender<Result<FlightData, Status>>,
        cursor: CursorImpl<StatementImpl<'s>>,
    ) -> Result<(), MyServerError> {
        let arrow_record_batches = OdbcReader::new(cursor, 100)
            //.map_err(arrow_odbc_err_to_status)?;
            .expect("failed to create odbc reader");

        for batchr in arrow_record_batches {
            let batch = batchr.expect("failed to fetch batch");

            let (dicts, batch) = flight_data_from_arrow_batch(&batch, &IpcWriteOptions::default());

            let rsp = response_sender.clone();
            task::spawn_blocking(move || {
                for dict in dicts {
                    let result = rsp.blocking_send(Ok(dict));
                    if let Err(_) = result {
                        log::error!("failed to send dict...");
                    }
                }
                let result = rsp.blocking_send(Ok(batch));
                if let Err(_) = result {
                    log::error!("failed to send data...");
                }
            });
        }

        Ok(())
    }
}

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and a `FlightData` representing the bytes of the batch's values
fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let data_gen = ipc::writer::IpcDataGenerator::default();
    let mut dictionary_tracker = ipc::writer::DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, options)
        .expect("DictionaryTracker configured above to not error.rs on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();

    (flight_dictionaries, flight_batch)
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
