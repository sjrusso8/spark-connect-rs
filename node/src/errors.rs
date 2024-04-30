use napi::bindgen_prelude::*;
use spark_connect_core::errors::SparkError;

#[derive(Debug)]
pub struct JsSparkError(napi::bindgen_prelude::Error);

impl From<JsSparkError> for napi::bindgen_prelude::Error {
    fn from(err: JsSparkError) -> Self {
        err.0
    }
}

impl From<SparkError> for JsSparkError {
    fn from(value: SparkError) -> Self {
        let err = Error::new(
            napi::bindgen_prelude::Status::GenericFailure,
            value.to_string(),
        );
        JsSparkError(err)
    }
}

#[allow(dead_code)]
impl JsSparkError {
    fn new_err<T: std::fmt::Display>(msg: T) -> Error {
        Error::new(
            napi::bindgen_prelude::Status::GenericFailure,
            msg.to_string(),
        )
    }
}
