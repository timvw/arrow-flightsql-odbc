use prost::Message;
use crate::arrow_flight_protocol::{FlightDescriptor, Ticket};
use crate::arrow_flight_protocol_sql::{CommandGetTables, CommandStatementQuery};
use crate::error::MyServerError;
use crate::error;
use crate::util::*;

#[derive(Debug, Clone)]
pub enum FlightSqlCommand {
    StatementQuery(CommandStatementQuery),
    GetTables(CommandGetTables),
}

impl FlightSqlCommand {

    fn try_parse_bytes<B: bytes::Buf>(buf: B) -> Result<FlightSqlCommand, MyServerError> {
        let any: prost_types::Any = prost::Message::decode(buf)
            .map_err(error::decode_error_to_status)?;

        match any {
            _ if any.is::<CommandStatementQuery>() => {
                let command = any.unpack()?
                    .expect("unreachable");
                Ok(FlightSqlCommand::StatementQuery(command))
            },
            _ if any.is::<CommandGetTables>() => {
                let command = any.unpack()
                    .map_err(error::arrow_error_to_status)?
                    .expect("unreachable");
                Ok(FlightSqlCommand::GetTables(command))
            },
            _ => Err(MyServerError::NotImplementedYet(format!("still need to implement support for {}", any.type_url))),
        }
    }

    pub fn try_parse_ticket(ticket: Ticket) -> Result<FlightSqlCommand, MyServerError> {
        FlightSqlCommand::try_parse_bytes(&*ticket.ticket)
    }

    pub fn try_parse_flight_descriptor(flight_descriptor: FlightDescriptor) -> Result<FlightSqlCommand, MyServerError> {
        FlightSqlCommand::try_parse_bytes(&*flight_descriptor.cmd)
    }

    pub fn to_ticket(&self) -> Ticket {
        let ticket = match self {
            FlightSqlCommand::StatementQuery(cmd) => cmd.as_any().encode_to_vec(),
            FlightSqlCommand::GetTables(cmd) => cmd.as_any().encode_to_vec(),
        };
        Ticket {
            ticket,
        }
    }
}
