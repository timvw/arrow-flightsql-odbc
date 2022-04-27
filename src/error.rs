#[derive(Debug)]
pub enum MyServerError {
    OdbcApiError(arrow_odbc::odbc_api::Error),
    TonicTransportError(tonic::transport::Error),
    TonicReflectionServerError(tonic_reflection::server::Error),
    AddrParseError(std::net::AddrParseError),
    ArrowOdbcError(arrow_odbc::Error),
    TonicStatus(tonic::Status),
    SendError(String),
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