use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use crate::dataframe::{DataFrame, DataFrameReader};
use crate::plan::LogicalPlanBuilder;
use crate::spark;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;

use spark::execute_plan_response::{ArrowBatch, Metrics};
use spark::expression::Literal;
use spark::spark_connect_service_client::SparkConnectServiceClient;
use spark::{DataType, ExecutePlanResponse};

use tokio::sync::Mutex;
use tonic::transport::{Channel, Error};
use tonic::Streaming;

use url::Url;
use uuid::Uuid;

/// ChannelBuilder validates a connection string
/// based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
struct ChannelBuilder {
    host: String,
    port: u16,
    token: Option<String>,
    user: Option<String>,
    headers: Option<HashMap<String, String>>,
}

impl Default for ChannelBuilder {
    fn default() -> Self {
        ChannelBuilder::build("sc://127.0.0.1:15002".to_string()).unwrap()
    }
}

impl ChannelBuilder {
    /// Build and Validate a connnection string
    pub fn build(connection: String) -> Result<ChannelBuilder, String> {
        let url =
            Url::parse(connection.as_str()).map_err(|_| "Failed to parse the url.".to_string())?;

        if url.scheme() != "sc" {
            return Err("Scheme is not set to 'sc'".to_string());
        };

        let host = url
            .host_str()
            .ok_or("Missing host in the URL.".to_string())?
            .to_string();

        let port = url.port().ok_or("Missing port in the URL.".to_string())?;

        let mut channel_builder = ChannelBuilder {
            host,
            port,
            token: None,
            user: None,
            headers: None,
        };

        let path: Vec<&str> = url.path().split(';').collect();

        if path.is_empty() || (path.len() == 1 && (path[0].is_empty() || path[0] == "/")) {
            return Ok(channel_builder);
        }

        let mut headers: HashMap<String, String> = path
            .into_iter()
            .filter(|&pair| (pair != "/") & (!pair.is_empty()))
            .map(|pair| {
                let mut parts = pair.splitn(2, '=');
                (
                    parts.next().unwrap_or("").to_string(),
                    parts.next().unwrap_or("").to_string(),
                )
            })
            .collect();

        if headers.is_empty() {
            return Ok(channel_builder);
        }

        channel_builder.token = headers.remove("token");
        channel_builder.user = headers.remove("user_id");
        channel_builder.headers = Some(headers);

        Ok(channel_builder)
    }
}

/// SparkSessionBuilder creates a remote Spark Session a connection string.
///
/// The connection string is define based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
pub struct SparkSessionBuilder {
    channel_builder: ChannelBuilder,
}

/// Default connects a Spark cluster running at `sc://127.0.0.1:15002/`
impl Default for SparkSessionBuilder {
    fn default() -> Self {
        let channel_builder = ChannelBuilder::default();

        Self { channel_builder }
    }
}

impl SparkSessionBuilder {
    fn new(connection: String) -> Self {
        let channel_builder = ChannelBuilder::build(connection).unwrap();

        Self { channel_builder }
    }

    /// Validate a connect string for a remote Spark Session
    ///
    /// String must conform to the [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
    pub fn remote(connection: String) -> Self {
        Self::new(connection)
    }

    /// Attempt to connect to a remote Spark Session
    ///
    /// and return a [SparkSession]
    pub async fn build(self) -> Result<SparkSession, Error> {
        let url = format!(
            "https://{}:{}",
            self.channel_builder.host, self.channel_builder.port
        );

        let client = Arc::new(Mutex::new(
            SparkConnectServiceClient::connect(url.clone()).await?,
        ));

        Ok(SparkSession {
            client,
            session_id: Uuid::new_v4().to_string(),
            metadata: self.channel_builder.headers,
            user: self.channel_builder.user,
            token: self.channel_builder.token,
        })
    }
}

/// The entry point to connecting to a Spark Cluster
/// using the Spark Connection gRPC protocol.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SparkSession {
    /// Spark Connection gRPC client interface
    pub client: Arc<Mutex<SparkConnectServiceClient<Channel>>>,

    /// Spark Session ID
    pub session_id: String,

    /// gRPC metadata collected from the connection string
    pub metadata: Option<HashMap<String, String>>,
    user: Option<String>,

    token: Option<String>,
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
        let range_relation = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "na".to_string(),
                plan_id: Some(1),
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
                user_id: self.user.clone().unwrap_or("NA".to_string()),
                user_name: self.user.clone().unwrap_or("NA".to_string()),
                extensions: vec![],
            }),
            operation_id: None,
            plan,
            client_type: Some("_SPARK_CONNECT_RUST".to_string()),
            request_options: vec![],
            tags: vec![],
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
        let mut stream = self
            .execute_plan(plan)
            .await
            .map_err(|err| ArrowError::IoError(err.to_string()))?;

        let mut handler = ResponseHandler::new();

        while let Some(resp) = stream
            .message()
            .await
            .map_err(|err| ArrowError::IoError(err.to_string()))?
        {
            let _ = handler.handle_response(&resp);
        }
        handler.records()
    }
}

struct ResponseHandler {
    schema: Option<DataType>,
    data: Vec<Option<ArrowBatch>>,
    metrics: Option<Metrics>,
}

impl Default for ResponseHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseHandler {
    fn new() -> ResponseHandler {
        ResponseHandler {
            schema: None,
            data: vec![],
            metrics: None,
        }
    }

    fn handle_response(&mut self, response: &ExecutePlanResponse) -> Result<(), String> {
        if let Some(schema) = response.schema.as_ref() {
            self.schema = Some(schema.clone());
        }
        if let Some(metrics) = response.metrics.as_ref() {
            self.metrics = Some(metrics.clone());
        }
        if let Some(data) = response.response_type.as_ref() {
            match data {
                spark::execute_plan_response::ResponseType::ArrowBatch(batch) => {
                    self.data.push(Some(batch.clone()));
                }
                _ => {
                    return Err("Not implemented".to_string());
                }
            }
        }
        Ok(())
    }

    fn records(self) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut accumulator: Vec<Vec<RecordBatch>> = vec![vec![]];
        for batch in self.data.into_iter().flatten() {
            accumulator.push(deserialize(batch)?);
        }

        Ok(accumulator
            .into_iter()
            .flatten()
            .collect::<Vec<RecordBatch>>())
    }
}

struct ArrowBatchReader {
    batch: ArrowBatch,
}

impl Read for ArrowBatchReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Read::read(&mut self.batch.data.as_slice(), buf)
    }
}

fn deserialize(batch: ArrowBatch) -> Result<Vec<RecordBatch>, ArrowError> {
    let wrapper = ArrowBatchReader { batch };
    let reader = StreamReader::try_new(wrapper, None)?;
    let mut rows = Vec::new();
    for record in reader {
        rows.push(record?)
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_builder_default() {
        let expected_url = "127.0.0.1:15002".to_string();

        let cb = ChannelBuilder::default();

        let output_url = format!("{}:{}", cb.host, cb.port);

        assert_eq!(expected_url, output_url)
    }

    #[test]
    #[should_panic(expected = "Scheme is not set to 'sc")]
    fn test_panic_incorrect_url_scheme() {
        let connection = "http://127.0.0.1:15002".to_string();

        ChannelBuilder::build(connection).unwrap();
    }

    #[test]
    #[should_panic(expected = "Failed to parse the url.")]
    fn test_panic_missing_url_host() {
        let connection = "sc://:15002".to_string();

        ChannelBuilder::build(connection).unwrap();
    }

    #[test]
    #[should_panic(expected = "Missing port in the URL")]
    fn test_panic_missing_url_port() {
        let connection = "sc://127.0.0.1".to_string();

        ChannelBuilder::build(connection).unwrap();
    }

    #[test]
    fn test_spark_session_builder() {
        let connection =
            "sc://myhost.com:443/;use_ssl=true;token=ABCDEFG;user_agent=some_agent;user_id=user123"
                .to_string();

        let ssbuilder = SparkSessionBuilder::remote(connection);

        assert_eq!("myhost.com".to_string(), ssbuilder.channel_builder.host);
        assert_eq!(443, ssbuilder.channel_builder.port);
        assert_eq!(
            "ABCDEFG".to_string(),
            ssbuilder.channel_builder.token.unwrap()
        );
        assert_eq!(
            "user123".to_string(),
            ssbuilder.channel_builder.user.unwrap()
        );
        assert_eq!(
            Some(&"true".to_string()),
            ssbuilder
                .channel_builder
                .headers
                .clone()
                .unwrap()
                .get("use_ssl")
        );
        assert_eq!(
            Some(&"some_agent".to_string()),
            ssbuilder
                .channel_builder
                .headers
                .clone()
                .unwrap()
                .get("user_agent")
        );
    }

    #[tokio::test]
    async fn test_spark_session_create() {
        let connection = "sc://localhost:15002/;use_ssl=true;token=ABCDEFG;user_agent=some_agent;user_id=user123".to_string();

        let spark = SparkSessionBuilder::remote(connection).build().await;

        assert!(spark.is_ok());
        assert_eq!(
            Some(&"true".to_string()),
            spark.unwrap().metadata.unwrap().get("use_ssl")
        );
    }
}
