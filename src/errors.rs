//! Defines a [SparkError] for representing failures in various Spark operations.
//! Most of these are wrappers for tonic or arrow error messages
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;

use std::error::Error;

use arrow::error::ArrowError;

/// Different `Spark` types
#[derive(Debug)]
pub enum SparkError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    ExternalError(Box<dyn Error + Send + Sync>),
    AnalysisException(String),
    IoError(String, std::io::Error),
    ArrowError(ArrowError),
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
        SparkError::AnalysisException(status.message().to_string())
    }
}

impl From<serde_json::Error> for SparkError {
    fn from(value: serde_json::Error) -> Self {
        SparkError::AnalysisException(value.to_string())
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
        }
    }
}

unsafe impl Send for SparkError {}
unsafe impl Sync for SparkError {}

impl Error for SparkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Self::ExternalError(e) = self {
            Some(e.as_ref())
        } else {
            None
        }
    }
}
