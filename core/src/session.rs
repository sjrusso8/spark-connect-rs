//! Spark Session containing the remote gRPC client

use std::collections::HashMap;
use std::sync::Arc;

use crate::client::{ChannelBuilder, HeadersLayer, SparkClient, SparkConnectClient};

use crate::catalog::Catalog;
use crate::conf::RunTimeConfig;
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::errors::SparkError;
use crate::plan::LogicalPlanBuilder;
use crate::streaming::DataStreamReader;

use crate::spark;
use spark::spark_connect_service_client::SparkConnectServiceClient;

use arrow::record_batch::RecordBatch;

use tokio::sync::RwLock;

use tower::ServiceBuilder;

#[cfg(not(feature = "wasm"))]
use tonic::transport::Channel;

#[cfg(feature = "wasm")]
use tonic_web_wasm_client::Client;

/// SparkSessionBuilder creates a remote Spark Session a connection string.
///
/// The connection string is define based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
pub struct SparkSessionBuilder {
    pub channel_builder: ChannelBuilder,
    configs: HashMap<String, String>,
}

/// Default connects a Spark cluster running at `sc://127.0.0.1:15002/`
impl Default for SparkSessionBuilder {
    fn default() -> Self {
        let channel_builder = ChannelBuilder::default();

        Self {
            channel_builder,
            configs: HashMap::new(),
        }
    }
}

impl SparkSessionBuilder {
    fn new(connection: &str) -> Self {
        let channel_builder = ChannelBuilder::create(connection).unwrap();

        Self {
            channel_builder,
            configs: HashMap::new(),
        }
    }

    /// Validate a connect string for a remote Spark Session
    ///
    /// String must conform to the [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
    pub fn remote(connection: &str) -> Self {
        Self::new(connection)
    }

    /// Sets a config option.
    pub fn config(mut self, key: &str, value: &str) -> Self {
        self.configs.insert(key.into(), value.into());
        self
    }

    /// Sets a name for the application, which will be shown in the Spark web UI.
    pub fn app_name(mut self, name: &str) -> Self {
        self.configs
            .insert("spark.app.name".to_string(), name.into());
        self
    }

    #[cfg(not(feature = "wasm"))]
    async fn create_client(&self) -> Result<SparkSession, SparkError> {
        let channel = Channel::from_shared(self.channel_builder.endpoint())?
            .connect()
            .await?;

        let channel = ServiceBuilder::new()
            .layer(HeadersLayer::new(
                self.channel_builder.headers().unwrap_or_default(),
            ))
            .service(channel);

        let client = SparkConnectServiceClient::new(channel);

        let spark_connnect_client =
            SparkConnectClient::new(Arc::new(RwLock::new(client)), self.channel_builder.clone());

        let mut rt_config = RunTimeConfig::new(&spark_connnect_client);

        rt_config.set_configs(&self.configs).await?;

        Ok(SparkSession::new(spark_connnect_client))
    }

    #[cfg(feature = "wasm")]
    async fn create_client(&self) -> Result<SparkSession, SparkError> {
        let inner = Client::new(self.channel_builder.endpoint());

        let service_client = SparkConnectServiceClient::with_interceptor(
            inner,
            MetadataInterceptor::new(
                self.channel_builder.token().to_owned(),
                self.channel_builder.headers().to_owned(),
            ),
        );

        let client = Arc::new(RwLock::new(service_client));

        let spark_connnect_client = SparkConnectClient::new(client, self.channel_builder.clone());

        let mut rt_config = RunTimeConfig::new(&spark_connnect_client);

        rt_config.set_configs(&self.configs).await?;

        Ok(SparkSession::new(spark_connnect_client))
    }

    /// Attempt to connect to a remote Spark Session
    ///
    /// and return a [SparkSession]
    pub async fn build(&self) -> Result<SparkSession, SparkError> {
        self.create_client().await
    }
}

/// The entry point to connecting to a Spark Cluster
/// using the Spark Connection gRPC protocol.
#[derive(Clone, Debug)]
pub struct SparkSession {
    #[cfg(not(feature = "wasm"))]
    client: SparkClient,
    // client: SparkConnectClient<InterceptedService<Channel, MetadataInterceptor>>,
    #[cfg(feature = "wasm")]
    client: SparkConnectClient<InterceptedService<Client, MetadataInterceptor>>,

    session_id: String,
}

impl SparkSession {
    #[cfg(not(feature = "wasm"))]
    pub fn new(client: SparkClient) -> Self {
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

    pub fn session(&self) -> SparkSession {
        self.clone()
    }

    /// Create a [DataFrame] with a spingle column named `id`,
    /// containing elements in a range from `start` (default 0) to
    /// `end` (exclusive) with a step value `step`, and control the number
    /// of partitions with `num_partitions`
    pub fn range(
        &self,
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

        DataFrame::new(self.session(), LogicalPlanBuilder::from(range_relation))
    }

    /// Returns a [DataFrameReader] that can be used to read datra in as a [DataFrame]
    pub fn read(&self) -> DataFrameReader {
        DataFrameReader::new(self.session())
    }

    /// Returns a [DataFrameReader] that can be used to read datra in as a [DataFrame]
    pub fn read_stream(&self) -> DataStreamReader {
        DataStreamReader::new(self.session())
    }

    pub fn table(&self, name: &str) -> Result<DataFrame, SparkError> {
        DataFrameReader::new(self.session()).table(name, None)
    }

    /// Interface through which the user may create, drop, alter or query underlying databases,
    /// tables, functions, etc.
    pub fn catalog(&self) -> Catalog {
        Catalog::new(self.session())
    }

    /// Returns a [DataFrame] representing the result of the given query
    pub async fn sql(&self, sql_query: &str) -> Result<DataFrame, SparkError> {
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

        Ok(DataFrame::new(self.session(), logical_plan))
    }

    pub fn create_dataframe(&self, data: &RecordBatch) -> Result<DataFrame, SparkError> {
        let logical_plan = LogicalPlanBuilder::local_relation(data)?;

        Ok(DataFrame::new(self.session(), logical_plan))
    }

    /// Return the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Spark Connection gRPC client interface
    #[cfg(not(feature = "wasm"))]
    pub fn client(self) -> SparkClient {
        self.client
    }

    #[cfg(feature = "wasm")]
    pub fn client(self) -> SparkConnectClient<InterceptedService<Client, MetadataInterceptor>> {
        self.client
    }

    /// Interrupt all operations of this session currently running on the connected server.
    pub async fn interrupt_all(&self) -> Result<Vec<String>, SparkError> {
        let resp = self
            .client
            .interrupt_request(spark::interrupt_request::InterruptType::All, None)
            .await?;

        Ok(resp.interrupted_ids)
    }

    /// Interrupt all operations of this session with the given operation tag.
    pub async fn interrupt_tag(&self, tag: &str) -> Result<Vec<String>, SparkError> {
        let resp = self
            .client
            .interrupt_request(
                spark::interrupt_request::InterruptType::Tag,
                Some(tag.to_string()),
            )
            .await?;

        Ok(resp.interrupted_ids)
    }

    /// Interrupt an operation of this session with the given operationId.
    pub async fn interrupt_operation(&self, op_id: &str) -> Result<Vec<String>, SparkError> {
        let resp = self
            .client
            .interrupt_request(
                spark::interrupt_request::InterruptType::OperationId,
                Some(op_id.to_string()),
            )
            .await?;

        Ok(resp.interrupted_ids)
    }

    /// Add a tag to be assigned to all the operations started by this thread in this session.
    pub fn add_tag(&mut self, tag: &str) -> Result<(), SparkError> {
        self.client.add_tag(tag)
    }

    /// Remove a tag previously added to be assigned to all the operations started by this thread in this session.
    pub fn remove_tag(&mut self, tag: &str) -> Result<(), SparkError> {
        self.client.remove_tag(tag)
    }

    /// Get the tags that are currently set to be assigned to all the operations started by this thread.
    pub fn get_tags(&mut self) -> &Vec<String> {
        self.client.get_tags()
    }

    /// Clear the current thread’s operation tags.
    pub fn clear_tags(&mut self) {
        self.client.clear_tags()
    }

    /// The version of Spark on which this application is running.
    pub async fn version(&self) -> Result<String, SparkError> {
        let version = spark::analyze_plan_request::Analyze::SparkVersion(
            spark::analyze_plan_request::SparkVersion {},
        );

        let mut client = self.client.clone();

        client.analyze(version).await?.spark_version()
    }

    // [RunTimeConfig] configuration interface for Spark.
    // pub fn conf(&self) -> RunTimeConfig {
    //     RunTimeConfig::new(&self.client)
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::{
        array::{ArrayRef, StringArray},
        record_batch::RecordBatch,
    };

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_test;session_id=0d2af2a9-cc3c-4d4b-bf27-e2fefeaca233";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[test]
    fn test_session_builder() {
        let connection = "sc://myhost.com:443/;token=ABCDEFG;user_agent=some_agent;user_id=user123";

        let ssbuilder = SparkSessionBuilder::remote(connection);

        assert_eq!(
            "http://myhost.com:443".to_string(),
            ssbuilder.channel_builder.endpoint()
        );
        assert_eq!(
            "Bearer ABCDEFG".to_string(),
            ssbuilder.channel_builder.token().unwrap()
        );
    }

    #[tokio::test]
    async fn test_spark_range() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 100, 1, Some(8));

        let records = df.collect().await?;

        assert_eq!(records.num_rows(), 100);
        Ok(())
    }

    #[tokio::test]
    async fn test_spark_create_dataframe() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));

        let record_batch = RecordBatch::try_from_iter(vec![("a", a)])?;

        let df = spark.create_dataframe(&record_batch)?;

        let rows = df.collect().await?;

        assert_eq!(record_batch, rows);
        Ok(())
    }

    #[tokio::test]
    async fn test_spark_session_create() {
        let connection =
            "sc://localhost:15002/;token=ABCDEFG;user_agent=some_agent;user_id=user123";

        let spark = SparkSessionBuilder::remote(connection).build().await;

        assert!(spark.is_ok());
    }

    #[tokio::test]
    async fn test_session_tags() -> Result<(), SparkError> {
        let mut spark = SparkSessionBuilder::default().build().await?;

        spark.add_tag("hello-tag")?;

        spark.add_tag("hello-tag-2")?;

        let expected = vec!["hello-tag".to_string(), "hello-tag-2".to_string()];

        let res = spark.get_tags();

        assert_eq!(&expected, res);

        spark.clear_tags();
        let res = spark.get_tags();

        let expected: Vec<String> = vec![];

        assert_eq!(&expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_session_tags_panic() -> Result<(), SparkError> {
        let mut spark = SparkSessionBuilder::default().build().await?;

        assert!(spark.add_tag("bad,tag").is_err());
        assert!(spark.add_tag("").is_err());

        assert!(spark.remove_tag("bad,tag").is_err());
        assert!(spark.remove_tag("").is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_session_version() -> Result<(), SparkError> {
        let spark = SparkSessionBuilder::default().build().await?;

        let version = spark.version().await?;

        assert_eq!("3.5.1".to_string(), version);
        Ok(())
    }
    //
    // #[tokio::test]
    // async fn test_session_config() -> Result<(), SparkError> {
    //     let value = "rust-test-app";
    //
    //     let spark = SparkSessionBuilder::default()
    //         .app_name("rust-test-app")
    //         .build()
    //         .await?;
    //
    //     let name = spark.conf().get("spark.app.name", None).await?;
    //
    //     assert_eq!(value, &name);
    //
    //     // validate set
    //     spark
    //         .conf()
    //         .set("spark.sql.shuffle.partitions", "42")
    //         .await?;
    //
    //     // validate get
    //     let val = spark
    //         .conf()
    //         .get("spark.sql.shuffle.partitions", None)
    //         .await?;
    //
    //     assert_eq!("42", &val);
    //
    //     // validate unset
    //     spark.conf().unset("spark.sql.shuffle.partitions").await?;
    //
    //     let val = spark
    //         .conf()
    //         .get("spark.sql.shuffle.partitions", None)
    //         .await?;
    //
    //     assert_eq!("200", &val);
    //
    //     // not a modifable setting
    //     let val = spark
    //         .conf()
    //         .is_modifable("spark.executor.instances")
    //         .await?;
    //     assert!(!val);
    //
    //     // a modifable setting
    //     let val = spark
    //         .conf()
    //         .is_modifable("spark.sql.shuffle.partitions")
    //         .await?;
    //     assert!(val);
    //
    //     Ok(())
    // }
}
