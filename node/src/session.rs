use spark_connect_core::session::{SparkSession, SparkSessionBuilder};

use crate::dataframe::JsDataFrame;
use crate::errors::JsSparkError;

#[napi(js_name = "SparkSession")]
pub struct JsSparkSession {
    pub(crate) spark: SparkSession,
}

impl JsSparkSession {
    fn new(spark: SparkSession) -> JsSparkSession {
        JsSparkSession { spark }
    }
}

#[napi]
impl JsSparkSession {
    #[napi(factory, catch_unwind)]
    pub async fn build(conn: String) -> napi::Result<JsSparkSession> {
        let spark = SparkSessionBuilder::remote(conn.as_str())
            .build()
            .await
            .map_err(JsSparkError::from)
            .unwrap();

        Ok(JsSparkSession::new(spark))
    }

    #[napi(catch_unwind)]
    pub async fn sql(&self, query: String) -> napi::Result<JsDataFrame> {
        let df = self
            .spark
            .clone()
            .sql(query.as_str())
            .await
            .map_err(JsSparkError::from)
            .unwrap();

        Ok(df.into())
    }
}
