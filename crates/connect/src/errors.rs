//! Defines a [SparkError] for representing failures in various Spark operations.
//! Most of these are wrappers for tonic or arrow error messages
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;

use arrow::error::ArrowError;

use tonic::Code;

#[cfg(feature = "datafusion")]
use datafusion::error::DataFusionError;
#[cfg(feature = "polars")]
use polars::error::PolarsError;

/// Different `Spark` types
#[derive(Debug)]
pub enum SparkError {
    /// Returned when functionality is not yet available.
    Aborted(String),
    AlreadyExists(String),
    AnalysisException(String),
    ArrowError(ArrowError),
    Cancelled(String),
    DataLoss(String),
    DeadlineExceeded(String),
    ExternalError(Box<dyn Error + Send + Sync>),
    FailedPrecondition(String),
    InvalidConnectionUrl(String),
    InvalidArgument(String),
    IoError(String, std::io::Error),
    NotFound(String),
    NotYetImplemented(String),
    PermissionDenied(String),
    ResourceExhausted(String),
    SessionNotSameException(String),
    Unauthenticated(String),
    Unavailable(String),
    Unknown(String),
    Unimplemented(String),
    Uuid(String),
    OutOfRange(String),
}

impl SparkError {
    /// Wraps an external error in an `SparkError`.
    pub fn from_external_error(error: Box<dyn Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }
}

impl From<std::io::Error> for SparkError {
    fn from(error: std::io::Error) -> Self {
        SparkError::IoError(error.to_string(), error)
    }
}

impl From<std::str::Utf8Error> for SparkError {
    fn from(error: std::str::Utf8Error) -> Self {
        SparkError::AnalysisException(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for SparkError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        SparkError::AnalysisException(error.to_string())
    }
}

impl From<ArrowError> for SparkError {
    fn from(error: ArrowError) -> Self {
        SparkError::ArrowError(error)
    }
}

impl From<tonic::Status> for SparkError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            Code::Ok => SparkError::AnalysisException(status.message().to_string()),
            Code::Unknown => SparkError::Unknown(status.message().to_string()),
            Code::Aborted => SparkError::Aborted(status.message().to_string()),
            Code::NotFound => SparkError::NotFound(status.message().to_string()),
            Code::Internal => SparkError::AnalysisException(status.message().to_string()),
            Code::DataLoss => SparkError::DataLoss(status.message().to_string()),
            Code::Cancelled => SparkError::Cancelled(status.message().to_string()),
            Code::OutOfRange => SparkError::OutOfRange(status.message().to_string()),
            Code::Unavailable => SparkError::Unavailable(status.message().to_string()),
            Code::AlreadyExists => SparkError::AnalysisException(status.message().to_string()),
            Code::InvalidArgument => SparkError::InvalidArgument(status.message().to_string()),
            Code::DeadlineExceeded => SparkError::DeadlineExceeded(status.message().to_string()),
            Code::Unimplemented => SparkError::Unimplemented(status.message().to_string()),
            Code::Unauthenticated => SparkError::Unauthenticated(status.message().to_string()),
            Code::PermissionDenied => SparkError::PermissionDenied(status.message().to_string()),
            Code::ResourceExhausted => SparkError::ResourceExhausted(status.message().to_string()),
            Code::FailedPrecondition => {
                SparkError::FailedPrecondition(status.message().to_string())
            }
        }
    }
}

impl From<serde_json::Error> for SparkError {
    fn from(value: serde_json::Error) -> Self {
        SparkError::AnalysisException(value.to_string())
    }
}

impl From<uuid::Error> for SparkError {
    fn from(value: uuid::Error) -> Self {
        SparkError::Uuid(value.to_string())
    }
}

#[cfg(feature = "datafusion")]
impl From<DataFusionError> for SparkError {
    fn from(_value: DataFusionError) -> Self {
        SparkError::AnalysisException("Error converting to DataFusion DataFrame".to_string())
    }
}

#[cfg(feature = "polars")]
impl From<PolarsError> for SparkError {
    fn from(_value: PolarsError) -> Self {
        SparkError::AnalysisException("Error converting to Polars DataFrame".to_string())
    }
}

impl From<tonic::codegen::http::uri::InvalidUri> for SparkError {
    fn from(value: tonic::codegen::http::uri::InvalidUri) -> Self {
        SparkError::InvalidConnectionUrl(value.to_string())
    }
}

impl From<tonic::transport::Error> for SparkError {
    fn from(value: tonic::transport::Error) -> Self {
        SparkError::InvalidConnectionUrl(value.to_string())
    }
}

impl<W: Write> From<std::io::IntoInnerError<W>> for SparkError {
    fn from(error: std::io::IntoInnerError<W>) -> Self {
        SparkError::IoError(error.to_string(), error.into())
    }
}

impl Display for SparkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SparkError::ExternalError(source) => write!(f, "External error: {}", &source),
            SparkError::AnalysisException(desc) => write!(f, "Analysis error: {desc}"),
            SparkError::IoError(desc, _) => write!(f, "Io error: {desc}"),
            SparkError::ArrowError(desc) => write!(f, "Apache Arrow error: {desc}"),
            SparkError::NotYetImplemented(source) => write!(f, "Not yet implemented: {source}"),
            SparkError::InvalidConnectionUrl(val) => write!(f, "Invalid URL error: {val}"),
            SparkError::SessionNotSameException(val) => {
                write!(f, "Spark Session ID is not the same: {val}")
            }
            SparkError::Aborted(val) => write!(f, "Aborted: {val}"),
            SparkError::AlreadyExists(val) => write!(f, "Already Exists: {val}"),
            SparkError::Cancelled(val) => write!(f, "Cancelled: {val}"),
            SparkError::DataLoss(val) => write!(f, "Data Loss: {val}"),
            SparkError::DeadlineExceeded(val) => write!(f, "Deadline Exceeded: {val}"),
            SparkError::FailedPrecondition(val) => write!(f, "Failed Precondition: {val}"),
            SparkError::InvalidArgument(val) => write!(f, "Invalid Argument: {val}"),
            SparkError::NotFound(val) => write!(f, "Not Found: {val}"),
            SparkError::PermissionDenied(val) => write!(f, "Permission Denied: {val}"),
            SparkError::ResourceExhausted(val) => write!(f, "Resource Exhausted: {val}"),
            SparkError::Unauthenticated(val) => write!(f, "Unauthenicated: {val}"),
            SparkError::Unavailable(val) => write!(f, "Unavailable: {val}"),
            SparkError::Unknown(val) => write!(f, "Unknown: {val}"),
            SparkError::Unimplemented(val) => write!(f, "Unimplemented: {val}"),
            SparkError::Uuid(val) => write!(f, "Invalid UUID {val}"),
            SparkError::OutOfRange(val) => write!(f, "Out Of Range: {val}"),
        }
    }
}

impl Error for SparkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Self::ExternalError(e) = self {
            Some(e.as_ref())
        } else {
            None
        }
    }
}
