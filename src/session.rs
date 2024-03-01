use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;

pub use crate::client::SparkSessionBuilder;
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::handler::ResponseHandler;
use crate::plan::LogicalPlanBuilder;
use crate::spark;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use spark::expression::Literal;
use spark::spark_connect_service_client::SparkConnectServiceClient;
use spark::{DataType, ExecutePlanResponse};

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
        let plan_id = Some(1);

        let range_relation = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "na".to_string(),
                plan_id,
            }),
            rel_type: Some(spark::relation::RelType::Range(spark::Range {
                start,
                end,
                step,
                num_partitions,
            })),
        };

        let logical_plan = LogicalPlanBuilder::new(range_relation);

        DataFrame::new(self, logical_plan)
    }

    /// Returns a [DataFrameReader] that can be used to read datra in as a [DataFrame]
    pub fn read(self) -> DataFrameReader {
        DataFrameReader::new(self)
    }

    /// Returns a [DataFrame] representing the result of the given query
    pub fn sql(self, sql_query: &str) -> DataFrame {
        let kind = Some(spark::data_type::Kind::Null(spark::data_type::Null {
            type_variation_reference: 1,
        }));

        let sql_command = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(1),
            }),
            rel_type: Some(spark::relation::RelType::Sql(spark::Sql {
                query: sql_query.to_string(),
                args: HashMap::new(),
                pos_args: vec![Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Null(DataType {
                        kind,
                    })),
                }],
            })),
        };

        let logical_plan = LogicalPlanBuilder::new(sql_command);

        DataFrame::new(self, logical_plan)
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

    async fn execute_plan(
        &mut self,
        plan: Option<spark::Plan>,
    ) -> Result<Streaming<ExecutePlanResponse>, tonic::Status> {
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
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut stream = self.execute_plan(plan).await.map_err(|err| {
            ArrowError::IoError(
                err.to_string(),
                Error::new(std::io::ErrorKind::Other, err.to_string()),
            )
        })?;

        let mut handler = ResponseHandler::new();

        while let Some(resp) = stream.message().await.map_err(|err| {
            ArrowError::IoError(
                err.to_string(),
                Error::new(std::io::ErrorKind::Other, err.to_string()),
            )
        })? {
            let _ = handler.handle_response(&resp);
        }
        handler.records()
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
}
