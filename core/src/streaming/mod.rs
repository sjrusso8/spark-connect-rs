//! Streaming implementation for the Spark Connect Client

use std::collections::HashMap;

use crate::plan::LogicalPlanBuilder;
use crate::session::SparkSession;
use crate::spark;
pub use crate::spark::write_stream_operation_start::Trigger;
use crate::DataFrame;

use crate::errors::SparkError;

/// DataStreamReader represents the entrypoint to create a streaming DataFrame
#[derive(Clone, Debug)]
pub struct DataStreamReader {
    spark_session: Box<SparkSession>,
    format: Option<String>,
    schema: Option<String>,
    read_options: HashMap<String, String>,
}

impl DataStreamReader {
    pub fn new(spark_session: SparkSession) -> Self {
        Self {
            spark_session: Box::new(spark_session),
            format: None,
            schema: None,
            read_options: HashMap::new(),
        }
    }

    /// Specifies the input data source format
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Schema of the stream in DDL format (e.g. `"name string, age int"`)
    pub fn schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    /// Add an input option for the underlying data source
    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.read_options.insert(key.to_string(), value.to_string());
        self
    }

    /// Set many input options based on an iterator of (key/value pairs) for the underlying data source
    pub fn options<I, K, V>(mut self, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.read_options = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();

        self
    }

    pub fn load(self, path: Option<&str>) -> Result<DataFrame, SparkError> {
        let paths = match path {
            Some(path) => vec![path.to_string()],
            None => vec![],
        };

        let read_type = Some(spark::relation::RelType::Read(spark::Read {
            is_streaming: true,
            read_type: Some(spark::read::ReadType::DataSource(spark::read::DataSource {
                format: self.format,
                schema: self.schema.to_owned(),
                options: self.read_options,
                paths,
                predicates: vec![],
            })),
        }));

        let relation = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(1),
            }),
            rel_type: read_type,
        };

        let plan = LogicalPlanBuilder::new(relation);

        Ok(DataFrame {
            spark_session: self.spark_session,
            plan,
        })
    }
}

/// Streaming Output Modes
#[derive(Clone, Debug)]
pub enum OutputMode {
    Append,
    Complete,
    Update,
}

impl OutputMode {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            OutputMode::Append => "append",
            OutputMode::Complete => "complete",
            OutputMode::Update => "update",
        }
    }
}

/// DataStreamWriter provides the ability to output a [StreamingQuery]
/// which can then be used to monitor the active stream
#[derive(Clone, Debug)]
pub struct DataStreamWriter {
    dataframe: DataFrame,
    format: Option<String>,
    output_mode: Option<OutputMode>,
    query_name: Option<String>,
    trigger: Option<spark::write_stream_operation_start::Trigger>,
    partition_by: Vec<String>,
    write_options: HashMap<String, String>,
}

impl DataStreamWriter {
    /// Create a new DataStreamWriter from a provided streaming [DataFrame]
    ///
    /// # Defaults
    /// - `format`: None,
    /// - `output_mode`: [OutputMode],
    /// - `query_name`: None,
    /// - `trigger`: [Trigger],
    /// - `partition_by`: vec![],
    /// - `write_options`: HashMap::new()
    ///
    pub fn new(dataframe: DataFrame) -> Self {
        Self {
            dataframe,
            format: None,
            output_mode: Some(OutputMode::Append),
            query_name: None,
            trigger: None,
            partition_by: vec![],
            write_options: HashMap::new(),
        }
    }

    /// Target format to output the [StreamingQuery]
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Specifies the behavior when data or table already exists
    ///
    /// # Arguments:
    /// - `output_mode`: [OutputMode] enum
    ///
    pub fn output_mode(mut self, output_mode: OutputMode) -> Self {
        self.output_mode = Some(output_mode);
        self
    }

    /// Partitions the output by the given columns on the file system
    pub fn partition_by<'a, I>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.partition_by = cols.into_iter().map(|col| col.to_string()).collect();
        self
    }

    /// Add an input option for the underlying data source
    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.write_options
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Set many input options based on an iterator of (key/value pairs) for the underlying data source
    pub fn options<I, K, V>(mut self, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.write_options = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();
        self
    }

    pub fn query_name(mut self, name: &str) -> Self {
        self.query_name = Some(name.to_string());
        self
    }

    /// Query trigger for data to be processed by
    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.trigger = Some(trigger);
        self
    }

    async fn start_stream(
        self,
        sink: Option<spark::write_stream_operation_start::SinkDestination>,
    ) -> Result<StreamingQuery, SparkError> {
        let ops = spark::WriteStreamOperationStart {
            input: Some(self.dataframe.plan.clone().relation()),
            format: self.format.unwrap_or("".to_string()),
            options: self.write_options,
            partitioning_column_names: self.partition_by,
            output_mode: self.output_mode.unwrap().as_str_name().to_string(),
            query_name: self.query_name.unwrap_or("".to_string()),
            foreach_batch: None,
            foreach_writer: None,
            trigger: self.trigger,
            sink_destination: sink,
        };

        let cmd = spark::command::CommandType::WriteStreamOperationStart(ops);

        let plan = LogicalPlanBuilder::plan_cmd(cmd);

        let mut client = self.dataframe.spark_session.clone().client();

        let operation_start_resp = client
            .execute_command_and_fetch(plan)
            .await?
            .write_stream_operation_start_result;

        Ok(StreamingQuery::new(
            self.dataframe.spark_session,
            operation_start_resp.unwrap(),
        ))
    }

    /// Start a streaming job to save the contents of the [StreamingQuery] to a data source.
    ///
    /// The data source is specified by the `format` and a set of `options`.
    pub async fn start(self, path: Option<&str>) -> Result<StreamingQuery, SparkError> {
        let sink = path.map(|path| {
            spark::write_stream_operation_start::SinkDestination::Path(path.to_string())
        });

        self.start_stream(sink).await
    }

    /// Start a streaming job to save the contents of the [StreamingQuery] to a table.
    pub async fn to_table(self, table_name: &str) -> Result<StreamingQuery, SparkError> {
        let sink = Some(
            spark::write_stream_operation_start::SinkDestination::TableName(table_name.to_string()),
        );

        self.start_stream(sink).await
    }
}

/// Represents the active streaming created from a `start` on the writer
///
/// This object is used to control and monitor the active stream
#[derive(Clone, Debug)]
pub struct StreamingQuery {
    spark_session: Box<SparkSession>,
    query_instance: spark::StreamingQueryInstanceId,
    query_id: String,
    run_id: String,
    name: Option<String>,
}

impl StreamingQuery {
    pub fn new(
        spark_session: Box<SparkSession>,
        write_stream: spark::WriteStreamOperationStartResult,
    ) -> Self {
        let query_instance = write_stream.query_id.unwrap();
        let query_id = query_instance.clone().id;
        let run_id = query_instance.clone().run_id;

        Self {
            spark_session,
            query_instance,
            query_id,
            run_id,
            name: Some(write_stream.name),
        }
    }

    fn streaming_query_cmd() -> spark::StreamingQueryCommand {
        spark::StreamingQueryCommand {
            query_id: None,
            command: None,
        }
    }

    async fn execute_query_cmd(
        &self,
        command: spark::StreamingQueryCommand,
    ) -> Result<spark::StreamingQueryCommandResult, SparkError> {
        let plan = LogicalPlanBuilder::plan_cmd(
            spark::command::CommandType::StreamingQueryCommand(command),
        );

        let mut client = self.spark_session.clone().client();

        client
            .execute_command_and_fetch(plan)
            .await?
            .streaming_query_command_result
            .ok_or_else(|| {
                SparkError::AnalysisException("Streaming Result Response is empty".to_string())
            })
    }

    pub fn id(&self) -> String {
        self.query_id.clone()
    }

    pub fn run_id(&self) -> String {
        self.run_id.clone()
    }

    pub fn name(&self) -> Option<String> {
        self.name.clone()
    }

    async fn fetch_status(
        &self,
    ) -> Result<spark::streaming_query_command_result::StatusResult, SparkError> {
        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::Status(true));

        let result_type = self
            .execute_query_cmd(command)
            .await?
            .result_type
            .ok_or_else(|| SparkError::AnalysisException("Stream status is empty".to_string()))?;

        match result_type {
            spark::streaming_query_command_result::ResultType::Status(status) => Ok(status),
            _ => Err(SparkError::AnalysisException(
                "Unexpected result type for stream status".to_string(),
            )),
        }
    }

    pub async fn await_termination(&self, timeout_ms: Option<i64>) -> Result<bool, SparkError> {
        let term = spark::streaming_query_command::AwaitTerminationCommand { timeout_ms };

        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::AwaitTermination(
            term,
        ));

        let result_type = self
            .execute_query_cmd(command)
            .await?
            .result_type
            .ok_or_else(|| {
                SparkError::AnalysisException("Stream termination status is empty".to_string())
            })?;

        let term = match result_type {
            spark::streaming_query_command_result::ResultType::AwaitTermination(term) => Ok(term),
            _ => Err(SparkError::AnalysisException(
                "Unexpected result type for termination request".to_string(),
            )),
        };

        Ok(term?.terminated)
    }

    pub async fn last_progress(&self) -> Result<serde_json::Value, SparkError> {
        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::LastProgress(true));

        let result_type = self
            .execute_query_cmd(command)
            .await?
            .result_type
            .ok_or_else(|| SparkError::AnalysisException("Stream progress is empty".to_string()))?;

        let progress = match result_type {
            spark::streaming_query_command_result::ResultType::RecentProgress(progress) => {
                Ok(progress)
            }
            _ => Err(SparkError::AnalysisException(
                "Unexpected result type for progress request".to_string(),
            )),
        };

        to_json_object(progress?.recent_progress_json)
    }

    pub async fn recent_progress(&self) -> Result<serde_json::Value, SparkError> {
        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::RecentProgress(
            true,
        ));

        let result_type = self
            .execute_query_cmd(command)
            .await?
            .result_type
            .ok_or_else(|| SparkError::AnalysisException("Stream progress is empty".to_string()))?;

        let progress = match result_type {
            spark::streaming_query_command_result::ResultType::RecentProgress(progress) => {
                Ok(progress)
            }
            _ => Err(SparkError::AnalysisException(
                "Unexpected result type for recent progress request".to_string(),
            )),
        };

        to_json_object(progress?.recent_progress_json)
    }

    pub async fn is_active(&self) -> Result<bool, SparkError> {
        let status = self.fetch_status().await?;

        Ok(status.is_active)
    }

    pub async fn stop(&self) -> Result<(), SparkError> {
        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::Stop(true));

        let _result_type = self.execute_query_cmd(command).await?;

        Ok(())
    }

    pub async fn process_all_available(&self) -> Result<(), SparkError> {
        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::ProcessAllAvailable(true));

        let _result_type = self.execute_query_cmd(command).await?;

        Ok(())
    }

    pub async fn explain(&self, extended: Option<bool>) -> Result<(), SparkError> {
        let extended = match extended {
            Some(true) => true,
            Some(false) => false,
            None => false,
        };

        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::Explain(
            spark::streaming_query_command::ExplainCommand { extended },
        ));

        let result_type = self
            .execute_query_cmd(command)
            .await?
            .result_type
            .ok_or_else(|| SparkError::AnalysisException("Stream explain is empty".to_string()))?;

        let explain = match result_type {
            spark::streaming_query_command_result::ResultType::Explain(explain) => Ok(explain),
            _ => Err(SparkError::AnalysisException(
                "Unexpected result type for progress request".to_string(),
            )),
        };

        Ok(println!("{}", explain?.result))
    }

    // !TODO i don't really like the return values on this
    pub async fn exception(&self) -> Result<String, SparkError> {
        let mut command = StreamingQuery::streaming_query_cmd();
        command.query_id = Some(self.query_instance.clone());
        command.command = Some(spark::streaming_query_command::Command::Exception(true));

        let result_type = self
            .execute_query_cmd(command)
            .await?
            .result_type
            .ok_or_else(|| {
                SparkError::AnalysisException("Stream exception is empty".to_string())
            })?;

        let exception = match result_type {
            spark::streaming_query_command_result::ResultType::Exception(exception) => {
                Ok(exception)
            }
            _ => Err(SparkError::AnalysisException(
                "Unexpected result type for recent progress request".to_string(),
            )),
        };

        match exception? {
            spark::streaming_query_command_result::ExceptionResult {
                exception_message: None,
                ..
            } => Ok("No exception captured".to_string()),
            spark::streaming_query_command_result::ExceptionResult {
                exception_message: Some(msg),
                error_class: Some(error_class),
                stack_trace: Some(stack_trace),
            } => {
                let msg = msg
                    + format!(
                        "\n\nError Class:\n{}\n\nJVM stacktrace:\n{}",
                        error_class, stack_trace
                    )
                    .as_str();

                Ok(msg)
            }
            spark::streaming_query_command_result::ExceptionResult {
                exception_message: Some(msg),
                error_class: Some(error_class),
                stack_trace: None,
            } => {
                let msg = msg + format!("\n\nError Class:\n{}", error_class).as_str();
                Ok(msg.to_string())
            }
            spark::streaming_query_command_result::ExceptionResult {
                exception_message: Some(msg),
                error_class: None,
                stack_trace: Some(stack_trace),
            } => {
                let msg = msg + format!("\n\nJVM stacktrace:\n{}", stack_trace).as_str();
                Ok(msg.to_string())
            }
            _ => Err(SparkError::AnalysisException(
                "Unexpected response from server".to_string(),
            )),
        }
    }

    pub async fn status(
        &self,
    ) -> Result<spark::streaming_query_command_result::StatusResult, SparkError> {
        self.fetch_status().await
    }
}

fn to_json_object(val: Vec<String>) -> Result<serde_json::Value, SparkError> {
    let val = &val.first().unwrap();
    Ok(serde_json::from_str::<serde_json::Value>(val)?)
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::{thread, time};

    use crate::errors::SparkError;
    use crate::SparkSessionBuilder;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_stream";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_read_stream() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .read_stream()
            .format("rate")
            .option("rowsPerSecond", "5")
            .load(None)?;

        assert!(df.is_streaming().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_active() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .read_stream()
            .format("rate")
            .option("rowsPerSecond", "5")
            .load(None)?;

        let query = df
            .write_stream()
            .format("memory")
            .query_name("TEST_ACTIVE")
            .output_mode(OutputMode::Append)
            .trigger(Trigger::ProcessingTimeInterval("3 seconds".to_string()))
            .start(None)
            .await?;

        assert!(query.is_active().await?);

        thread::sleep(time::Duration::from_secs(10));

        query.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_status() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .read_stream()
            .format("rate")
            .option("rowsPerSecond", "5")
            .load(None)?;

        let query = df
            .write_stream()
            .format("memory")
            .query_name("TEST_STATUS")
            .output_mode(OutputMode::Append)
            .trigger(Trigger::ProcessingTimeInterval("3 seconds".to_string()))
            .start(None)
            .await?;

        let status = query.status().await?;

        assert!(!status.status_message.is_empty());

        query.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_progress() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .read_stream()
            .format("rate")
            .option("rowsPerSecond", "5")
            .load(None)?;

        let query = df
            .write_stream()
            .format("memory")
            .query_name("TEST_PROGRESS")
            .output_mode(OutputMode::Append)
            .trigger(Trigger::ProcessingTimeInterval("1 seconds".to_string()))
            .start(None)
            .await?;

        thread::sleep(time::Duration::from_secs(5));

        let progress = query.last_progress().await?;

        assert!(!progress.is_null());

        query.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_explain() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .read_stream()
            .format("rate")
            .option("rowsPerSecond", "5")
            .load(None)?;

        let query = df
            .write_stream()
            .format("memory")
            .query_name("TEST_EXPLAIN")
            .start(None)
            .await?;

        thread::sleep(time::Duration::from_secs(3));

        query.process_all_available().await?;

        assert!(query.explain(None).await.is_ok());

        query.stop().await?;

        Ok(())
    }
}
