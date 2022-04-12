/*
pub mod arrow_flight_protocol_sql {
    tonic::include_proto!("arrow.flight.protocol.sql"); // The string specified here must match the proto package name
}*/

#[path = "arrow.flight.protocol.rs"]
pub mod arrow_flight_protocol;

#[path = "arrow.flight.protocol.sql.rs"]
pub mod arrow_flight_protocol_sql;

pub mod myserver;