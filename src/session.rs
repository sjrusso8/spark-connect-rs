//! Spark Session containing the remote gRPC client

use std::collections::HashMap;

use crate::catalog::Catalog;
pub use crate::client::SparkSessionBuilder;
use crate::client::{MetadataInterceptor, SparkConnectClient};
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::errors::SparkError;
use crate::plan::LogicalPlanBuilder;
use crate::spark;
use crate::streaming::DataStreamReader;

use arrow::record_batch::RecordBatch;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// The entry point to connecting to a Spark Cluster
/// using the Spark Connection gRPC protocol.
#[derive(Clone, Debug)]
pub struct SparkSession {
    client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,

    session_id: String,
}

impl SparkSession {
    pub fn new(
        client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,
    ) -> Self {
        Self {
            session_id: client.session_id(),
            client,
        }
    }
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

    /// Returns a [DataFrameReader] that can be used to read datra in as a [DataFrame]
    #[allow(non_snake_case)]
    pub fn readStream(self) -> DataStreamReader {
        DataStreamReader::new(self)
    }

    pub fn table(self, name: &str) -> Result<DataFrame, SparkError> {
        DataFrameReader::new(self).table(name, None)
    }

    /// Interface through which the user may create, drop, alter or query underlying databases,
    /// tables, functions, etc.
    pub fn catalog(self) -> Catalog {
        Catalog::new(self)
    }

    /// Returns a [DataFrame] representing the result of the given query
    pub async fn sql(self, sql_query: &str) -> Result<DataFrame, SparkError> {
        let sql_cmd = spark::command::CommandType::SqlCommand(spark::SqlCommand {
            sql: sql_query.to_string(),
            args: HashMap::default(),
            pos_args: vec![],
        });

        let plan = LogicalPlanBuilder::plan_cmd(sql_cmd);

        let resp = self
            .clone()
            .client()
            .execute_command_and_fetch(plan)
            .await?;

        let relation = resp.sql_command_result.to_owned().unwrap().relation;

        let logical_plan = LogicalPlanBuilder::new(relation.unwrap());

        Ok(DataFrame::new(self, logical_plan))
    }

    #[allow(non_snake_case)]
    pub fn createDataFrame(self, data: &RecordBatch) -> Result<DataFrame, SparkError> {
        let logical_plan = LogicalPlanBuilder::local_relation(data)?;
        Ok(DataFrame::new(self, logical_plan))
    }

    /// Return the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Spark Connection gRPC client interface
    pub fn client(self) -> SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>> {
        self.client
    }
}
