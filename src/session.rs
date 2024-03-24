//! Spark Session containing the remote gRPC client

use std::collections::HashMap;

use crate::catalog::Catalog;
pub use crate::client::SparkSessionBuilder;
use crate::client::{MetadataInterceptor, SparkConnectClient};
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::errors::SparkError;
use crate::plan::LogicalPlanBuilder;
use crate::spark;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// The entry point to connecting to a Spark Cluster
/// using the Spark Connection gRPC protocol.
#[derive(Clone, Debug)]
pub struct SparkSession {
    /// Spark Connection gRPC client interface
    pub client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,
}

impl SparkSession {
    pub fn new(
        client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,
    ) -> Self {
        Self { client }
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

    /// Interface through which the user may create, drop, alter or query underlying databases,
    /// tables, functions, etc.
    pub fn catalog(self) -> Catalog {
        Catalog::new(self)
    }

    /// Returns a [DataFrame] representing the result of the given query
    pub async fn sql(&mut self, sql_query: &str) -> Result<DataFrame, SparkError> {
        let sql_cmd = spark::command::CommandType::SqlCommand(spark::SqlCommand {
            sql: sql_query.to_string(),
            args: HashMap::default(),
            pos_args: vec![],
        });

        let plan = LogicalPlanBuilder::build_plan_cmd(sql_cmd);

        self.client.execute_command(plan).await?;

        let cmd = self.client.handler.sql_command_result.to_owned().unwrap();

        let logical_plan = LogicalPlanBuilder::new(cmd.relation.unwrap());

        Ok(DataFrame::new(self.clone(), logical_plan))
    }
}
