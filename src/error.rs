#[derive(Debug)]
pub enum MyServerError {
    OdbcApiError(arrow_odbc::odbc_api::Error),
    TonicTransportError(tonic::transport::Error),
    TonicReflectionServerError(tonic_reflection::server::Error),
    AddrParseError(std::net::AddrParseError),
    ArrowError(arrow::error::ArrowError),
    ArrowOdbcError(arrow_odbc::Error),
    TonicStatus(tonic::Status),
    SendError(String),
    NotImplementedYet(String),
}

impl From<arrow_odbc::odbc_api::Error> for MyServerError {
    fn from(error: arrow_odbc::odbc_api::Error) -> Self {
        MyServerError::OdbcApiError(error)
    }
}

impl From<tonic::transport::Error> for MyServerError {
    fn from(error: tonic::transport::Error) -> Self {
        MyServerError::TonicTransportError(error)
    }
}

impl From<tonic_reflection::server::Error> for MyServerError {
    fn from(error: tonic_reflection::server::Error) -> Self {
        MyServerError::TonicReflectionServerError(error)
    }
}

impl From<std::net::AddrParseError> for MyServerError {
    fn from(error: std::net::AddrParseError) -> Self {
        MyServerError::AddrParseError(error)
    }
}

impl From<arrow::error::ArrowError> for MyServerError {
    fn from(error: arrow::error::ArrowError) -> Self {
        MyServerError::ArrowError(error)
    }
}

impl From<arrow_odbc::Error> for MyServerError {
    fn from(error: arrow_odbc::Error) -> Self {
        MyServerError::ArrowOdbcError(error)
    }
}

impl From<tonic::Status> for MyServerError {
    fn from(error: tonic::Status) -> Self {
        MyServerError::TonicStatus(error)
    }
}

pub fn arrow_error_to_status(err: arrow::error::ArrowError) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", err))
}

pub fn decode_error_to_status(err: prost::DecodeError) -> tonic::Status {
    tonic::Status::invalid_argument(format!("{:?}", err))
}
