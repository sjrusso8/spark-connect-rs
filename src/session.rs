//! Spark Session containing the remote gRPC client

use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;

use crate::catalog::Catalog;
pub use crate::client::SparkSessionBuilder;
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::errors::SparkError;
use crate::handler::ResponseHandler;
use crate::plan::LogicalPlanBuilder;
use crate::spark;

use arrow::record_batch::RecordBatch;

use spark::spark_connect_service_client::SparkConnectServiceClient;
use spark::ExecutePlanResponse;

use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Streaming;

/// The entry point to connecting to a Spark Cluster
/// using the Spark Connection gRPC protocol.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SparkSession {
    /// Spark Connection gRPC client interface
    pub client: Arc<
        Mutex<
            SparkConnectServiceClient<
                tonic::service::interceptor::InterceptedService<
                    Channel,
                    crate::client::MetadataInterceptor,
                >,
            >,
        >,
    >,

    /// Spark Session ID
    pub session_id: String,

    /// gRPC metadata collected from the connection string
    pub metadata: Option<HashMap<String, String>>,
    pub user_id: Option<String>,

    pub token: Option<&'static str>,
}

impl SparkSession {
    /// Create a [DataFrame] with a spingle column named `id`,
    /// containing elements in a range from `start` (default 0) to
    /// `end` (exclusive) with a step value `step`, and control the number
    /// of partitions with `num_partitions`
    pub fn range(
        self,
        start: Option<i64>,
        end: i64,
        step: i64,
        num_partitions: Option<i32>,
    ) -> DataFrame {
        let range_relation = spark::relation::RelType::Range(spark::Range {
            start,
            end,
            step,
            num_partitions,
        });

        DataFrame::new(self, LogicalPlanBuilder::from(range_relation))
    }

    /// Returns a [DataFrameReader] that can be used to read datra in as a [DataFrame]
    pub fn read(self) -> DataFrameReader {
        DataFrameReader::new(self)
    }

    /// Interface through which the user may create, drop, alter or query underlying databases,
    /// tables, functions, etc.
    pub fn catalog(self) -> Catalog {
        Catalog::new(self)
    }

    /// Returns a [DataFrame] representing the result of the given query
    pub async fn sql(&mut self, sql_query: &str) -> Result<DataFrame, SparkError> {
        let error_msg = SparkError::AnalysisException(
            "Failed to get command response from Spark Connect Server".to_string(),
        );

        let sql_cmd = spark::command::CommandType::SqlCommand(spark::SqlCommand {
            sql: sql_query.to_string(),
            args: HashMap::default(),
            pos_args: vec![],
        });

        let plan = LogicalPlanBuilder::build_plan_cmd(sql_cmd);

        let resp = self.execute_plan(Some(plan)).await?.message().await?;

        match resp.ok_or(error_msg)?.response_type {
            Some(spark::execute_plan_response::ResponseType::SqlCommandResult(sql_result)) => {
                let logical_plan = LogicalPlanBuilder::new(sql_result.relation.unwrap());
                Ok(DataFrame::new(self.clone(), logical_plan))
            }
            _ => Err(SparkError::NotYetImplemented(
                "Response type not implemented".to_string(),
            )),
        }
    }

    fn build_execute_plan_request(&self, plan: Option<spark::Plan>) -> spark::ExecutePlanRequest {
        spark::ExecutePlanRequest {
            session_id: self.session_id.clone(),
            user_context: Some(spark::UserContext {
                user_id: self.user_id.clone().unwrap_or("NA".to_string()),
                user_name: self.user_id.clone().unwrap_or("NA".to_string()),
                extensions: vec![],
            }),
            operation_id: None,
            plan,
            client_type: Some("_SPARK_CONNECT_RUST".to_string()),
            request_options: vec![],
            tags: vec![],
        }
    }

    fn build_analyze_plan_request(
        &self,
        analyze: Option<spark::analyze_plan_request::Analyze>,
    ) -> spark::AnalyzePlanRequest {
        spark::AnalyzePlanRequest {
            session_id: self.session_id.clone(),
            user_context: Some(spark::UserContext {
                user_id: self.user_id.clone().unwrap_or("NA".to_string()),
                user_name: self.user_id.clone().unwrap_or("NA".to_string()),
                extensions: vec![],
            }),
            client_type: Some("_SPARK_CONNECT_RUST".to_string()),
            analyze,
        }
    }

    pub async fn execute_plan(
        &mut self,
        plan: Option<spark::Plan>,
    ) -> Result<Streaming<ExecutePlanResponse>, SparkError> {
        let exc_plan = self.build_execute_plan_request(plan);

        let mut client = self.client.lock().await;

        let value = client.execute_plan(exc_plan).await?.into_inner();

        Ok(value)
    }

    /// Call a service on the remote Spark Connect server by running
    /// a provided [spark::Plan].
    ///
    /// A [spark::Plan] produces a vector of [RecordBatch] records
    pub async fn consume_plan(
        &mut self,
        plan: Option<spark::Plan>,
    ) -> Result<Vec<RecordBatch>, SparkError> {
        let mut stream = self.execute_plan(plan).await?;

        let mut handler = ResponseHandler::new();

        while let Some(resp) = stream.message().await.map_err(|err| {
            SparkError::IoError(
                err.to_string(),
                Error::new(std::io::ErrorKind::Other, err.to_string()),
            )
        })? {
            let _ = handler.handle_response(&resp);
        }
        Ok(handler.records().unwrap())
    }

    pub async fn analyze_plan(
        &mut self,
        analyze: Option<spark::analyze_plan_request::Analyze>,
    ) -> Option<spark::analyze_plan_response::Result> {
        let request = self.build_analyze_plan_request(analyze);
        let mut client = self.client.lock().await;

        let stream = client.analyze_plan(request).await.unwrap();

        stream.into_inner().result
    }

    pub async fn consume_plan_and_fetch(&mut self, plan: Option<spark::Plan>) -> Option<String> {
        let result = self
            .consume_plan(plan)
            .await
            .expect("Failed to get a result from Spark Connect");

        let col = result[0].column(0);

        let data: &arrow::array::StringArray = match col.data_type() {
            arrow::datatypes::DataType::Utf8 => col.as_any().downcast_ref().unwrap(),
            _ => unimplemented!("only Utf8 data types are currently handled currently."),
        };

        Some(data.value(0).to_string())
    }
}
