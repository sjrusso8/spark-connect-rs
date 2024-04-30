use crate::errors::JsSparkError;
use spark_connect_core::dataframe::DataFrame;

#[napi(js_name = "DataFrame")]
#[derive(Clone)]
pub struct JsDataFrame {
    pub(crate) df: Option<DataFrame>,
}

impl JsDataFrame {
    pub(crate) fn new(df: DataFrame) -> JsDataFrame {
        JsDataFrame { df: Some(df) }
    }
}

impl From<DataFrame> for JsDataFrame {
    fn from(df: DataFrame) -> JsDataFrame {
        JsDataFrame::new(df)
    }
}

#[napi]
impl JsDataFrame {
    #[napi(constructor, catch_unwind)]
    pub fn empty() -> JsDataFrame {
        JsDataFrame { df: None }
    }

    #[napi(catch_unwind)]
    pub fn select(&self, cols: Vec<&str>) -> napi::Result<JsDataFrame> {
        if let Some(df) = &self.df {
            Ok(df.clone().select(cols).into())
        } else {
            Err(napi::Error::new(
                napi::Status::GenericFailure,
                "DF was empty",
            ))
        }
    }

    #[napi(catch_unwind)]
    pub fn filter(&self, expr: String) -> napi::Result<JsDataFrame> {
        if let Some(df) = &self.df {
            Ok(df.clone().filter(expr.as_str()).into())
        } else {
            Err(napi::Error::new(
                napi::Status::GenericFailure,
                "DF was empty",
            ))
        }
    }

    #[napi(catch_unwind)]
    pub async fn count(&self) -> napi::Result<i64> {
        if let Some(df) = &self.df {
            Ok(df.clone().count().await.unwrap())
        } else {
            Err(napi::Error::new(
                napi::Status::GenericFailure,
                "DF was empty",
            ))
        }
    }

    #[napi(catch_unwind)]
    pub async fn show(&self) {
        if let Some(df) = &self.df {
            df.clone()
                .show(Some(10), None, None)
                .await
                .map_err(JsSparkError::from)
                .unwrap()
        }
    }
}
