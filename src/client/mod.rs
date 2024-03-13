use std::collections::HashMap;
use std::sync::Arc;

use crate::spark;
use crate::SparkSession;

use spark::spark_connect_service_client::SparkConnectServiceClient;

use tokio::sync::Mutex;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Endpoint, Error};
use tonic::Status;

use url::Url;

/// ChannelBuilder validates a connection string
/// based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
pub struct ChannelBuilder {
    pub host: String,
    pub port: u16,
    pub token: Option<&'static str>,
    pub user_id: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}

impl Default for ChannelBuilder {
    fn default() -> Self {
        ChannelBuilder::create("sc://127.0.0.1:15002".to_string()).unwrap()
    }
}

impl ChannelBuilder {
    /// create and Validate a connnection string
    pub fn create(connection: String) -> Result<ChannelBuilder, String> {
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
            user_id: None,
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

        if let Some(token) = headers.remove("token") {
            channel_builder.token = Some(Box::leak(token.into_boxed_str()));
        }
        if let Some(user_id) = headers.remove("user_id") {
            channel_builder.user_id = Some(user_id.to_string())
        }

        channel_builder.headers = Some(headers);

        Ok(channel_builder)
    }

    async fn create_client(&self) -> Result<SparkSession, Error> {
        let endpoint = format!("https://{}:{}", self.host, self.port);

        let channel = Endpoint::from_shared(endpoint)?.connect().await?;

        let service_client = SparkConnectServiceClient::with_interceptor(
            channel,
            MetadataInterceptor {
                token: Arc::new(self.token),
            },
        );

        let client = Arc::new(Mutex::new(service_client));

        Ok(SparkSession::new(
            client,
            self.headers.clone(),
            self.user_id.clone(),
            self.token,
        ))
    }
}

pub struct MetadataInterceptor {
    token: Arc<Option<&'static str>>,
}

impl Interceptor for MetadataInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(token) = *self.token {
            req.metadata_mut()
                .insert("authorization", AsciiMetadataValue::from_static(token));
        }

        Ok(req)
    }
}

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
    fn new(connection: String) -> Self {
        let channel_builder = ChannelBuilder::create(connection).unwrap();

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
        self.channel_builder.create_client().await
    }
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

        ChannelBuilder::create(connection).unwrap();
    }

    #[test]
    #[should_panic(expected = "Failed to parse the url.")]
    fn test_panic_missing_url_host() {
        let connection = "sc://:15002".to_string();

        ChannelBuilder::create(connection).unwrap();
    }

    #[test]
    #[should_panic(expected = "Missing port in the URL")]
    fn test_panic_missing_url_port() {
        let connection = "sc://127.0.0.1".to_string();

        ChannelBuilder::create(connection).unwrap();
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
            ssbuilder.channel_builder.user_id.unwrap()
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
    }
}
