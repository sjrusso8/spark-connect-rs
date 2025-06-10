// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a [SparkError] for representing failures in various Spark operations.
//! Most of these are wrappers for tonic or arrow error messages
use std::error::Error;
use std::fmt::Debug;
use std::io::Write;

use arrow::error::ArrowError;
use thiserror::Error;

use tonic::Code;

#[cfg(feature = "datafusion")]
use datafusion::error::DataFusionError;
#[cfg(feature = "polars")]
use polars::error::PolarsError;

/// Different `Spark` Error types
#[derive(Error, Debug)]
pub enum SparkError {
    #[error("Aborted: {0}")]
    Aborted(String),

    #[error("Already Exists: {0}")]
    AlreadyExists(String),

    #[error("Analysis Exception: {0}")]
    AnalysisException(String),

    #[error("Apache Arrow Error: {0}")]
    ArrowError(#[from] ArrowError),

    #[error("Cancelled: {0}")]
    Cancelled(String),

    #[error("Data Loss Exception: {0}")]
    DataLoss(String),

    #[error("Deadline Exceeded: {0}")]
    DeadlineExceeded(String),

    #[error("External Error: {0}")]
    ExternalError(Box<dyn Error + Send + Sync>),

    #[error("Failed Precondition: {0}")]
    FailedPrecondition(String),

    #[error("Invalid Connection Url: {0}")]
    InvalidConnectionUrl(String),

    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),

    #[error("Io Error: {0}")]
    IoError(String, std::io::Error),

    #[error("Not Found: {0}")]
    NotFound(String),

    #[error("Not Yet Implemented: {0}")]
    NotYetImplemented(String),

    #[error("Permission Denied: {0}")]
    PermissionDenied(String),

    #[error("Resource Exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Spark Session ID is not the same: {0}")]
    SessionNotSameException(String),

    #[error("Unauthenticated: {0}")]
    Unauthenticated(String),

    #[error("Unavailable: {0}")]
    Unavailable(String),

    #[error("Unkown: {0}")]
    Unknown(String),

    #[error("Unimplemented; {0}")]
    Unimplemented(String),

    #[error("Invalid UUID")]
    Uuid(#[from] uuid::Error),

    #[error("Out of Range: {0}")]
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
