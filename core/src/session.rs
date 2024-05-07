//! Spark Session containing the remote gRPC client

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::plan::LogicalPlanBuilder;
use crate::spark;
use crate::streaming::DataStreamReader;

use crate::client::{ChannelBuilder, MetadataInterceptor, SparkConnectClient};
use crate::errors::SparkError;
use spark::spark_connect_service_client::SparkConnectServiceClient;

use arrow::record_batch::RecordBatch;

use parking_lot::RwLock;

#[cfg(not(feature = "wasm"))]
use tonic::transport::{Channel, Endpoint};

#[cfg(feature = "wasm")]
use tonic_web_wasm_client::Client;

use tonic::service::interceptor::InterceptedService;

/// SparkSessionBuilder creates a remote Spark Session a connection string.
///
/// The connection string is define based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
pub struct SparkSessionBuilder {
    pub channel_builder: ChannelBuilder,
}

/// Default connects a Spark cluster running at `sc://127.0.0.1:15002/`
impl Default for SparkSessionBuilder {
    fn default() -> Self {
        let channel_builder = ChannelBuilder::default();

        Self { channel_builder }
    }
}

impl SparkSessionBuilder {
    fn new(connection: &str) -> Self {
        let channel_builder = ChannelBuilder::create(connection).unwrap();

        Self { channel_builder }
    }

    /// Validate a connect string for a remote Spark Session
    ///
    /// String must conform to the [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
    pub fn remote(connection: &str) -> Self {
        Self::new(connection)
    }

    #[cfg(not(feature = "wasm"))]
    async fn create_client(&self) -> Result<SparkSession, SparkError> {
        let channel = Endpoint::from_shared(self.channel_builder.endpoint())
            .expect("Failed to create endpoint")
            .connect()
            .await
            .expect("Failed to create channel");

        let service_client = SparkConnectServiceClient::with_interceptor(
            channel,
            MetadataInterceptor::new(
                self.channel_builder.token().to_owned(),
                self.channel_builder.headers().to_owned(),
            ),
        );

        let client = Arc::new(RwLock::new(service_client));

        let spark_connnect_client =
            SparkConnectClient::new(client.clone(), self.channel_builder.clone());

        Ok(SparkSession::new(spark_connnect_client))
    }

    #[cfg(feature = "wasm")]
    async fn create_client(&self) -> Result<SparkSession, SparkError> {
        // Need to switch to http for the test on wasi to work
        // Unsure if this is always needed for wasm
        let channel = self.channel_builder.endpoint().replace("https", "http");

        let inner = Client::new(channel);

        let service_client = SparkConnectServiceClient::with_interceptor(
            inner,
            MetadataInterceptor::new(
                self.channel_builder.token().to_owned(),
                self.channel_builder.headers().to_owned(),
            ),
        );

        let client = Arc::new(RwLock::new(service_client));

        let spark_connnect_client =
            SparkConnectClient::new(client.clone(), self.channel_builder.clone());

        Ok(SparkSession::new(spark_connnect_client))
    }

    /// Attempt to connect to a remote Spark Session
    ///
    /// and return a [SparkSession]
    pub async fn build(self) -> Result<SparkSession, SparkError> {
        self.create_client().await
    }
}

/// The entry point to connecting to a Spark Cluster
/// using the Spark Connection gRPC protocol.
#[derive(Clone, Debug)]
pub struct SparkSession {
    #[cfg(not(feature = "wasm"))]
    client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,

    #[cfg(feature = "wasm")]
    client: SparkConnectClient<InterceptedService<Client, MetadataInterceptor>>,

    session_id: String,
}

impl SparkSession {
    #[cfg(not(feature = "wasm"))]
    pub fn new(
        client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,
    ) -> Self {
        Self {
            session_id: client.session_id(),
            client,
        }
    }

    #[cfg(feature = "wasm")]
    pub fn new(
        client: SparkConnectClient<InterceptedService<Client, MetadataInterceptor>>,
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
    #[cfg(not(feature = "wasm"))]
    pub fn client(self) -> SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>> {
        self.client
    }

    #[cfg(feature = "wasm")]
    pub fn client(self) -> SparkConnectClient<InterceptedService<Client, MetadataInterceptor>> {
        self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spark_session_builder() {
        let connection = "sc://myhost.com:443/;token=ABCDEFG;user_agent=some_agent;user_id=user123";

        let ssbuilder = SparkSessionBuilder::remote(connection);

        assert_eq!(
            "https://myhost.com:443".to_string(),
            ssbuilder.channel_builder.endpoint()
        );
        assert_eq!(
            "Bearer ABCDEFG".to_string(),
            ssbuilder.channel_builder.token().unwrap()
        );
    }

    #[tokio::test]
    async fn test_spark_session_create() {
        let connection =
            "sc://localhost:15002/;token=ABCDEFG;user_agent=some_agent;user_id=user123";

        let spark = SparkSessionBuilder::remote(connection).build().await;

        assert!(spark.is_ok());
    }
}
