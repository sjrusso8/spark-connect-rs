//! Generic implementation of ChannelBuilder and SparkConnectClient

use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;

use crate::errors::SparkError;
use crate::spark;
use spark::execute_plan_response::ResponseType;

use spark::spark_connect_service_client::SparkConnectServiceClient;

use arrow::compute::concat_batches;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;

use tonic::codegen::{Body, Bytes, StdError};
use tonic::metadata::{
    Ascii, AsciiMetadataValue, KeyAndValueRef, MetadataKey, MetadataMap, MetadataValue,
};
use tonic::service::Interceptor;
use tonic::Status;

use tokio::sync::RwLock;

use url::Url;

use uuid::Uuid;

type Host = String;
type Port = u16;
type UrlParse = (Host, Port, Option<HashMap<String, String>>);

/// ChannelBuilder validates a connection string
/// based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
pub struct ChannelBuilder {
    host: Host,
    port: Port,
    session_id: Uuid,
    token: Option<String>,
    user_id: Option<String>,
    user_agent: Option<String>,
    use_ssl: bool,
    headers: Option<MetadataMap>,
}

impl Default for ChannelBuilder {
    fn default() -> Self {
        let connection = match env::var("SPARK_REMOTE") {
            Ok(conn) => conn.to_string(),
            Err(_) => "sc://localhost:15002".to_string(),
        };

        ChannelBuilder::create(&connection).unwrap()
    }
}

impl ChannelBuilder {
    pub fn new() -> Self {
        ChannelBuilder::default()
    }

    pub fn endpoint(&self) -> String {
        let scheme = if cfg!(feature = "tls") {
            "https"
        } else {
            "http"
        };

        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    pub fn token(&self) -> Option<String> {
        self.token.to_owned()
    }

    pub fn headers(&self) -> Option<MetadataMap> {
        self.headers.to_owned()
    }

    fn create_user_agent(user_agent: Option<&str>) -> Option<String> {
        let user_agent = user_agent.unwrap_or("_SPARK_CONNECT_RUST");
        let pkg_version = env!("CARGO_PKG_VERSION");
        let os = env::consts::OS.to_lowercase();

        Some(format!(
            "{} os/{} spark_connect_rs/{}",
            user_agent, os, pkg_version
        ))
    }

    fn create_user_id(user_id: Option<&str>) -> Option<String> {
        match user_id {
            Some(user_id) => Some(user_id.to_string()),
            None => match env::var("USER") {
                Ok(user) => Some(user),
                Err(_) => None,
            },
        }
    }

    pub fn parse_connection_string(connection: &str) -> Result<UrlParse, SparkError> {
        let url = Url::parse(connection).map_err(|_| {
            SparkError::InvalidConnectionUrl("Failed to parse the connection URL".to_string())
        })?;

        if url.scheme() != "sc" {
            return Err(SparkError::InvalidConnectionUrl(
                "The URL must start with 'sc://'. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'".to_string(),
            ));
        };

        let host = url
            .host_str()
            .ok_or_else(|| {
                SparkError::InvalidConnectionUrl(
                    "The hostname must not be empty. Please update
                    the URL to follow the correct format, e.g., 'sc://hostname:port'."
                        .to_string(),
                )
            })?
            .to_string();

        let port = url.port().ok_or_else(|| {
            SparkError::InvalidConnectionUrl(
                "The port must not be empty. Please update
                    the URL to follow the correct format, e.g., 'sc://hostname:port'."
                    .to_string(),
            )
        })?;

        let headers = ChannelBuilder::parse_headers(url);

        Ok((host, port, headers))
    }

    pub fn parse_headers(url: Url) -> Option<HashMap<String, String>> {
        let path: Vec<&str> = url
            .path()
            .split(';')
            .filter(|&pair| (pair != "/") & (!pair.is_empty()))
            .collect();

        if path.is_empty() || (path.len() == 1 && (path[0].is_empty() || path[0] == "/")) {
            return None;
        }

        let headers: HashMap<String, String> = path
            .iter()
            .copied()
            .map(|pair| {
                let mut parts = pair.splitn(2, '=');
                (
                    parts.next().unwrap_or("").to_string(),
                    parts.next().unwrap_or("").to_string(),
                )
            })
            .collect();

        if headers.is_empty() {
            return None;
        }

        Some(headers)
    }

    /// Create and validate a connnection string
    #[allow(unreachable_code)]
    pub fn create(connection: &str) -> Result<ChannelBuilder, SparkError> {
        let (host, port, headers) = ChannelBuilder::parse_connection_string(connection)?;

        let mut channel_builder = ChannelBuilder {
            host,
            port,
            session_id: Uuid::new_v4(),
            token: None,
            user_id: ChannelBuilder::create_user_id(None),
            user_agent: ChannelBuilder::create_user_agent(None),
            use_ssl: false,
            headers: None,
        };

        let mut headers = match headers {
            Some(headers) => headers,
            None => return Ok(channel_builder),
        };

        channel_builder.user_id = headers
            .remove("user_id")
            .map(|user_id| ChannelBuilder::create_user_id(Some(&user_id)))
            .unwrap_or_else(|| ChannelBuilder::create_user_id(None));

        channel_builder.user_agent = headers
            .remove("user_agent")
            .map(|user_agent| ChannelBuilder::create_user_agent(Some(&user_agent)))
            .unwrap_or_else(|| ChannelBuilder::create_user_agent(None));

        if let Some(token) = headers.remove("token") {
            channel_builder.token = Some(format!("Bearer {token}"));
        }

        if let Some(session_id) = headers.remove("session_id") {
            channel_builder.session_id = Uuid::from_str(&session_id).unwrap()
        }

        if let Some(use_ssl) = headers.remove("use_ssl") {
            if use_ssl.to_lowercase() == "true" {
                #[cfg(not(feature = "tls"))]
                {
                    panic!(
                        "The 'use_ssl' option requires the 'tls' feature, but it's not enabled!"
                    );
                };
                channel_builder.use_ssl = true
            }
        };

        channel_builder.headers = Some(metadata_builder(&headers));

        Ok(channel_builder)
    }
}

#[derive(Clone, Debug)]
pub struct MetadataInterceptor {
    token: Option<String>,
    metadata: Option<MetadataMap>,
}

impl Interceptor for MetadataInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(header) = &self.metadata {
            merge_metadata(req.metadata_mut(), header);
        }
        if let Some(token) = &self.token {
            req.metadata_mut().insert(
                "authorization",
                AsciiMetadataValue::from_str(token.as_str()).unwrap(),
            );
        }

        Ok(req)
    }
}

impl MetadataInterceptor {
    pub fn new(token: Option<String>, metadata: Option<MetadataMap>) -> Self {
        MetadataInterceptor { token, metadata }
    }
}

fn metadata_builder(headers: &HashMap<String, String>) -> MetadataMap {
    let mut metadata_map = MetadataMap::new();
    for (key, val) in headers.iter() {
        let meta_val = MetadataValue::from_str(val.as_str()).unwrap();
        let meta_key = MetadataKey::from_str(key.as_str()).unwrap();

        metadata_map.insert(meta_key, meta_val);
    }

    metadata_map
}

fn merge_metadata(metadata_into: &mut MetadataMap, metadata_from: &MetadataMap) {
    metadata_for_each(metadata_from, |key, value| {
        if key.to_string().starts_with("x-") {
            metadata_into.insert(key, value.to_owned());
        }
    })
}

fn metadata_for_each<F>(metadata: &MetadataMap, mut f: F)
where
    F: FnMut(&MetadataKey<Ascii>, &MetadataValue<Ascii>),
{
    for kv_ref in metadata.iter() {
        match kv_ref {
            KeyAndValueRef::Ascii(key, value) => f(key, value),
            KeyAndValueRef::Binary(_key, _value) => {}
        }
    }
}

#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
pub struct ResponseHandler {
    session_id: Option<String>,
    operation_id: Option<String>,
    response_id: Option<String>,
    metrics: Option<spark::execute_plan_response::Metrics>,
    observed_metrics: Option<spark::execute_plan_response::ObservedMetrics>,
    pub(crate) schema: Option<spark::DataType>,
    batches: Vec<RecordBatch>,
    pub(crate) sql_command_result: Option<spark::execute_plan_response::SqlCommandResult>,
    pub(crate) write_stream_operation_start_result: Option<spark::WriteStreamOperationStartResult>,
    pub(crate) streaming_query_command_result: Option<spark::StreamingQueryCommandResult>,
    pub(crate) get_resources_command_result: Option<spark::GetResourcesCommandResult>,
    pub(crate) streaming_query_manager_command_result:
        Option<spark::StreamingQueryManagerCommandResult>,
    pub(crate) result_complete: bool,
    total_count: isize,
}

#[derive(Default, Debug, Clone)]
pub struct AnalyzeHandler {
    pub(crate) schema: Option<spark::DataType>,
    pub(crate) explain: Option<String>,
    pub(crate) tree_string: Option<String>,
    pub(crate) is_local: Option<bool>,
    pub(crate) is_streaming: Option<bool>,
    pub(crate) input_files: Option<Vec<String>>,
    pub(crate) spark_version: Option<String>,
    pub(crate) ddl_parse: Option<spark::DataType>,
    pub(crate) same_semantics: Option<bool>,
    pub(crate) semantic_hash: Option<i32>,
    pub(crate) get_storage_level: Option<spark::StorageLevel>,
}

impl ResponseHandler {
    fn new() -> Self {
        Self {
            session_id: None,
            operation_id: None,
            response_id: None,
            metrics: None,
            observed_metrics: None,
            schema: None,
            batches: Vec::new(),
            sql_command_result: None,
            write_stream_operation_start_result: None,
            streaming_query_command_result: None,
            get_resources_command_result: None,
            streaming_query_manager_command_result: None,
            result_complete: false,
            total_count: 0,
        }
    }
}

impl AnalyzeHandler {
    fn new() -> Self {
        Self {
            schema: None,
            explain: None,
            tree_string: None,
            is_local: None,
            is_streaming: None,
            input_files: None,
            spark_version: None,
            ddl_parse: None,
            same_semantics: None,
            semantic_hash: None,
            get_storage_level: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SparkConnectClient<T> {
    stub: Arc<RwLock<SparkConnectServiceClient<T>>>,
    builder: ChannelBuilder,
    pub(crate) handler: ResponseHandler,
    pub(crate) analyzer: AnalyzeHandler,
    pub(crate) user_context: Option<spark::UserContext>,
    pub tags: Vec<String>,
    pub use_reattachable_execute: bool,
}

impl<T> SparkConnectClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub fn new(stub: Arc<RwLock<SparkConnectServiceClient<T>>>, builder: ChannelBuilder) -> Self {
        let user_ref = builder.user_id.clone().unwrap_or("".to_string());

        SparkConnectClient {
            stub,
            builder,
            handler: ResponseHandler::new(),
            analyzer: AnalyzeHandler::new(),
            user_context: Some(spark::UserContext {
                user_id: user_ref.clone(),
                user_name: user_ref,
                extensions: vec![],
            }),
            tags: vec![],
            use_reattachable_execute: true,
        }
    }

    pub fn session_id(&self) -> String {
        self.builder.session_id.to_string()
    }

    pub fn set_reattachable_execute(&mut self, setting: bool) -> Result<(), SparkError> {
        self.use_reattachable_execute = setting;
        Ok(())
    }

    fn request_options(&self) -> Vec<spark::execute_plan_request::RequestOption> {
        if self.use_reattachable_execute {
            let reattach_opt = spark::ReattachOptions { reattachable: true };
            let request_opt = spark::execute_plan_request::RequestOption {
                request_option: Some(
                    spark::execute_plan_request::request_option::RequestOption::ReattachOptions(
                        reattach_opt,
                    ),
                ),
            };

            return vec![request_opt];
        };

        vec![]
    }

    fn execute_plan_request_with_metadata(&self) -> spark::ExecutePlanRequest {
        spark::ExecutePlanRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: None,
            plan: None,
            client_type: self.builder.user_agent.clone(),
            request_options: self.request_options(),
            tags: self.tags.clone(),
        }
    }

    fn analyze_plan_request_with_metadata(&self) -> spark::AnalyzePlanRequest {
        spark::AnalyzePlanRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.builder.user_agent.clone(),
            analyze: None,
        }
    }

    async fn execute_and_fetch(
        &mut self,
        req: spark::ExecutePlanRequest,
    ) -> Result<(), SparkError> {
        let mut client = self.stub.write().await;

        let mut stream = client.execute_plan(req).await?.into_inner();
        drop(client);

        // clear out any prior responses
        self.handler = ResponseHandler::new();

        while let Some(_resp) = match stream.message().await {
            Ok(Some(msg)) => {
                // Handle the message
                self.handle_response(msg.clone())?;

                Some(msg)
            }
            Ok(None) => {
                if self.use_reattachable_execute && !self.handler.result_complete {
                    println!("didn't get the whole stream submitting a reattach");
                    if let Err(err) = self.reattach_request().await {
                        return Err(err);
                    }
                }
                None
            }
            Err(err) => {
                if self.use_reattachable_execute && !self.handler.result_complete {
                    println!("Error occurred, attempting to reattach the request");
                    // Attempt to reattach the request before returning the error
                    if let Err(err) = self.reattach_request().await {
                        // If reattach fails, return the original error
                        return Err(SparkError::IoError(
                            err.to_string(),
                            std::io::Error::new(std::io::ErrorKind::Other, err.to_string()),
                        ));
                    }
                    None // Retry after reattaching
                } else {
                    return Err(SparkError::IoError(
                        err.to_string(),
                        std::io::Error::new(std::io::ErrorKind::Other, err.to_string()),
                    ));
                }
            }
        } {}
        //
        // if stream.message().await.is_ok() {
        //     println!("next message")
        // }

        Ok(())
    }

    pub async fn analyze(
        &mut self,
        analyze: spark::analyze_plan_request::Analyze,
    ) -> Result<&mut Self, SparkError> {
        let mut req = self.analyze_plan_request_with_metadata();

        req.analyze = Some(analyze);

        // clear out any prior responses
        self.analyzer = AnalyzeHandler::new();

        let mut client = self.stub.write().await;
        let resp = client.analyze_plan(req).await?.into_inner();
        drop(client);

        self.handle_analyze(resp)
    }

    fn validate_tag(&self, tag: &str) -> Result<(), SparkError> {
        if tag.contains(',') {
            return Err(SparkError::AnalysisException(
                "Spark Connect tag can not contain ',' ".to_string(),
            ));
        };

        if tag.is_empty() {
            return Err(SparkError::AnalysisException(
                "Spark Connect tag can not an empty string ".to_string(),
            ));
        };

        Ok(())
    }

    pub fn add_tag(&mut self, tag: &str) -> Result<(), SparkError> {
        self.validate_tag(tag)?;
        self.tags.push(tag.to_string());
        Ok(())
    }

    pub fn remove_tag(&mut self, tag: &str) -> Result<(), SparkError> {
        self.validate_tag(tag)?;
        self.tags.retain(|t| t != tag);
        Ok(())
    }

    pub fn get_tags(&self) -> &Vec<String> {
        &self.tags
    }

    pub fn clear_tags(&mut self) {
        self.tags = vec![];
    }

    async fn reattach_request(&mut self) -> Result<(), SparkError> {
        let mut client = self.stub.write().await;

        let req = spark::ReattachExecuteRequest {
            session_id: self.handler.session_id.clone().unwrap(),
            user_context: self.user_context.clone(),
            operation_id: self.handler.operation_id.clone().unwrap(),
            client_type: self.builder.user_agent.clone(),
            last_response_id: self.handler.response_id.clone(),
        };

        let mut stream = client.reattach_execute(req).await?.into_inner();
        drop(client);

        // clear out any prior responses
        // self.handler = ResponseHandler::new();

        while let Some(_resp) = match stream.message().await {
            Ok(Some(msg)) => {
                // Handle the message
                self.handle_response(msg.clone())?;

                Some(msg)
            }
            Ok(None) => {
                if self.use_reattachable_execute && !self.handler.result_complete {
                    println!("something bad happened in the reattach. submitting reattach");
                    if let Err(err) = Box::pin(self.reattach_request()).await {
                        return Err(err);
                    }
                }
                None
            }
            Err(err) => {
                return Err(SparkError::IoError(
                    err.to_string(),
                    std::io::Error::new(std::io::ErrorKind::Other, err.to_string()),
                ));
            }
        } {}

        Ok(())
    }

    pub async fn config_request(
        &self,
        operation: spark::config_request::Operation,
    ) -> Result<spark::ConfigResponse, SparkError> {
        let operation = spark::ConfigRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.builder.user_agent.clone(),
            operation: Some(operation),
        };

        let mut client = self.stub.write().await;

        let resp = client.config(operation).await?.into_inner();

        Ok(resp)
    }

    pub async fn interrupt_request(
        &self,
        interrupt_type: spark::interrupt_request::InterruptType,
        id_or_tag: Option<String>,
    ) -> Result<spark::InterruptResponse, SparkError> {
        let mut req = spark::InterruptRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.builder.user_agent.clone(),
            interrupt_type: 0,
            interrupt: None,
        };

        match interrupt_type {
            spark::interrupt_request::InterruptType::All => {
                req.interrupt_type = interrupt_type.into();
            }
            spark::interrupt_request::InterruptType::Tag => {
                let tag = id_or_tag.expect("Tag can not be empty");
                let interrupt = spark::interrupt_request::Interrupt::OperationTag(tag);
                req.interrupt_type = interrupt_type.into();
                req.interrupt = Some(interrupt);
            }
            spark::interrupt_request::InterruptType::OperationId => {
                let op_id = id_or_tag.expect("Operation ID can not be empty");
                let interrupt = spark::interrupt_request::Interrupt::OperationId(op_id);
                req.interrupt_type = interrupt_type.into();
                req.interrupt = Some(interrupt);
            }
            spark::interrupt_request::InterruptType::Unspecified => {
                return Err(SparkError::AnalysisException(
                    "Interrupt Type was not specified".to_string(),
                ))
            }
        };

        let mut client = self.stub.write().await;

        let resp = client.interrupt(req).await?.into_inner();

        Ok(resp)
    }

    fn handle_response(&mut self, resp: spark::ExecutePlanResponse) -> Result<(), SparkError> {
        println!("{:?}", resp);
        self.validate_session(&resp.session_id)?;

        self.handler.session_id = Some(resp.session_id);
        self.handler.operation_id = Some(resp.operation_id);
        self.handler.response_id = Some(resp.response_id);

        if let Some(schema) = &resp.schema {
            self.handler.schema = Some(schema.clone());
        }
        if let Some(metrics) = &resp.metrics {
            self.handler.metrics = Some(metrics.clone());
        }
        if let Some(data) = resp.response_type {
            match data {
                ResponseType::ArrowBatch(res) => {
                    self.deserialize(res.data.as_slice(), res.row_count)?
                }
                ResponseType::SqlCommandResult(sql_cmd) => {
                    self.handler.sql_command_result = Some(sql_cmd.clone())
                }
                ResponseType::WriteStreamOperationStartResult(write_stream_op) => {
                    self.handler.write_stream_operation_start_result = Some(write_stream_op)
                }
                ResponseType::StreamingQueryCommandResult(stream_qry_cmd) => {
                    self.handler.streaming_query_command_result = Some(stream_qry_cmd)
                }
                ResponseType::GetResourcesCommandResult(resource_cmd) => {
                    self.handler.get_resources_command_result = Some(resource_cmd)
                }
                ResponseType::StreamingQueryManagerCommandResult(stream_qry_mngr_cmd) => {
                    self.handler.streaming_query_manager_command_result = Some(stream_qry_mngr_cmd)
                }
                ResponseType::ResultComplete(_) => self.handler.result_complete = true,
                ResponseType::Extension(_) => {
                    unimplemented!("extension response types are not implemented")
                }
            }
        }
        Ok(())
    }

    fn handle_analyze(
        &mut self,
        resp: spark::AnalyzePlanResponse,
    ) -> Result<&mut Self, SparkError> {
        self.validate_session(&resp.session_id)?;
        if let Some(result) = resp.result {
            match result {
                spark::analyze_plan_response::Result::Schema(schema) => {
                    self.analyzer.schema = schema.schema
                }
                spark::analyze_plan_response::Result::Explain(explain) => {
                    self.analyzer.explain = Some(explain.explain_string)
                }
                spark::analyze_plan_response::Result::TreeString(tree_string) => {
                    self.analyzer.tree_string = Some(tree_string.tree_string)
                }
                spark::analyze_plan_response::Result::IsLocal(is_local) => {
                    self.analyzer.is_local = Some(is_local.is_local)
                }
                spark::analyze_plan_response::Result::IsStreaming(is_streaming) => {
                    self.analyzer.is_streaming = Some(is_streaming.is_streaming)
                }
                spark::analyze_plan_response::Result::InputFiles(input_files) => {
                    self.analyzer.input_files = Some(input_files.files)
                }
                spark::analyze_plan_response::Result::SparkVersion(spark_version) => {
                    self.analyzer.spark_version = Some(spark_version.version)
                }
                spark::analyze_plan_response::Result::DdlParse(ddl_parse) => {
                    self.analyzer.ddl_parse = ddl_parse.parsed
                }
                spark::analyze_plan_response::Result::SameSemantics(same_semantics) => {
                    self.analyzer.same_semantics = Some(same_semantics.result)
                }
                spark::analyze_plan_response::Result::SemanticHash(semantic_hash) => {
                    self.analyzer.semantic_hash = Some(semantic_hash.result)
                }
                spark::analyze_plan_response::Result::Persist(_) => {}
                spark::analyze_plan_response::Result::Unpersist(_) => {}
                spark::analyze_plan_response::Result::GetStorageLevel(level) => {
                    self.analyzer.get_storage_level = level.storage_level
                }
            }
        }

        Ok(self)
    }

    fn validate_session(&self, session_id: &str) -> Result<(), SparkError> {
        if self.builder.session_id.to_string() != session_id {
            return Err(SparkError::AnalysisException(format!(
                "Received incorrect session identifier for request: {0} != {1}",
                self.builder.session_id, session_id
            )));
        }
        Ok(())
    }

    fn deserialize(&mut self, res: &[u8], row_count: i64) -> Result<(), SparkError> {
        let reader = StreamReader::try_new(res, None)?;
        for batch in reader {
            let record = batch?;
            if record.num_rows() != row_count as usize {
                return Err(SparkError::ArrowError(ArrowError::IpcError(format!(
                    "Expected {} rows in arrow batch but got {}",
                    row_count,
                    record.num_rows()
                ))));
            };
            self.handler.batches.push(record);
            self.handler.total_count += row_count as isize;
        }
        Ok(())
    }

    pub async fn execute_command(&mut self, plan: spark::Plan) -> Result<(), SparkError> {
        let mut req = self.execute_plan_request_with_metadata();

        req.plan = Some(plan);

        self.execute_and_fetch(req).await?;

        Ok(())
    }

    pub async fn execute_command_and_fetch(
        &mut self,
        plan: spark::Plan,
    ) -> Result<ResponseHandler, SparkError> {
        let mut req = self.execute_plan_request_with_metadata();

        req.plan = Some(plan);

        self.execute_and_fetch(req).await?;

        Ok(self.handler.clone())
    }

    #[allow(clippy::wrong_self_convention)]
    pub async fn to_arrow(&mut self, plan: spark::Plan) -> Result<RecordBatch, SparkError> {
        let mut req = self.execute_plan_request_with_metadata();

        req.plan = Some(plan);

        self.execute_and_fetch(req).await?;

        Ok(concat_batches(
            &self.handler.batches[0].schema(),
            &self.handler.batches,
        )?)
    }

    #[allow(clippy::wrong_self_convention)]
    pub async fn to_first_value(&mut self, plan: spark::Plan) -> Result<String, SparkError> {
        let rows = self.to_arrow(plan).await?;
        let col = rows.column(0);

        let data: &arrow::array::StringArray = match col.data_type() {
            arrow::datatypes::DataType::Utf8 => col.as_any().downcast_ref().unwrap(),
            _ => unimplemented!("only Utf8 data types are currently handled currently."),
        };

        Ok(data.value(0).to_string())
    }

    pub fn schema(&self) -> Result<spark::DataType, SparkError> {
        self.analyzer
            .schema
            .to_owned()
            .ok_or_else(|| SparkError::AnalysisException("Schema response is empty".to_string()))
    }

    pub fn explain(&self) -> Result<String, SparkError> {
        self.analyzer
            .explain
            .to_owned()
            .ok_or_else(|| SparkError::AnalysisException("Explain response is empty".to_string()))
    }

    pub fn tree_string(&self) -> Result<String, SparkError> {
        self.analyzer.tree_string.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Tree String response is empty".to_string())
        })
    }

    pub fn is_local(&self) -> Result<bool, SparkError> {
        self.analyzer
            .is_local
            .to_owned()
            .ok_or_else(|| SparkError::AnalysisException("Is Local response is empty".to_string()))
    }

    pub fn is_streaming(&self) -> Result<bool, SparkError> {
        self.analyzer.is_streaming.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Is Streaming response is empty".to_string())
        })
    }

    pub fn input_files(&self) -> Result<Vec<String>, SparkError> {
        self.analyzer.input_files.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Input Files response is empty".to_string())
        })
    }

    pub fn spark_version(&mut self) -> Result<String, SparkError> {
        self.analyzer.spark_version.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Spark Version resonse is empty".to_string())
        })
    }

    pub fn ddl_parse(&self) -> Result<spark::DataType, SparkError> {
        self.analyzer
            .ddl_parse
            .to_owned()
            .ok_or_else(|| SparkError::AnalysisException("DDL parse response is empty".to_string()))
    }

    pub fn same_semantics(&self) -> Result<bool, SparkError> {
        self.analyzer.same_semantics.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Same Semantics response is empty".to_string())
        })
    }

    pub fn semantic_hash(&self) -> Result<i32, SparkError> {
        self.analyzer.semantic_hash.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Semantic Hash response is empty".to_string())
        })
    }

    pub fn get_storage_level(&self) -> Result<spark::StorageLevel, SparkError> {
        self.analyzer.get_storage_level.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Storage Level response is empty".to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_builder_default() {
        let expected_url = "http://localhost:15002".to_string();

        let cb = ChannelBuilder::default();

        assert_eq!(expected_url, cb.endpoint())
    }

    #[test]
    fn test_panic_incorrect_url_scheme() {
        let connection = "http://127.0.0.1:15002";

        assert!(ChannelBuilder::create(connection).is_err())
    }

    #[test]
    fn test_panic_missing_url_host() {
        let connection = "sc://:15002";

        assert!(ChannelBuilder::create(connection).is_err())
    }

    #[test]
    fn test_panic_missing_url_port() {
        let connection = "sc://127.0.0.1";

        assert!(ChannelBuilder::create(connection).is_err())
    }

    #[test]
    #[should_panic(
        expected = "The 'use_ssl' option requires the 'tls' feature, but it's not enabled!"
    )]
    fn test_panic_ssl() {
        let connection = "sc://127.0.0.1:443/;use_ssl=true";

        ChannelBuilder::create(&connection).unwrap();
    }
}
