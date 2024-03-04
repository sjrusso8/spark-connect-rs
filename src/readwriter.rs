//! DataFrameReader & DataFrameWriter representations

use std::collections::HashMap;

use crate::plan::LogicalPlanBuilder;
use crate::session::SparkSession;
use crate::spark;
use crate::DataFrame;

use spark::write_operation::SaveMode;

use arrow::error::ArrowError;

/// DataFrameReader represents the entrypoint to create a DataFrame
/// from a specific file format.
#[derive(Clone, Debug)]
pub struct DataFrameReader {
    spark_session: SparkSession,
    format: Option<String>,
    read_options: HashMap<String, String>,
}

impl DataFrameReader {
    /// Create a new DataFrameReader with a [SparkSession]
    pub fn new(spark_session: SparkSession) -> Self {
        Self {
            spark_session,
            format: None,
            read_options: HashMap::new(),
        }
    }

    /// Specifies the input data source format
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Add an input option for the underlying data source
    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.read_options.insert(key.to_string(), value.to_string());
        self
    }

    /// Set many input options based on a [HashMap] for the underlying data source
    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.read_options = options;
        self
    }

    /// Loads data from a data source and returns it as a [DataFrame]
    ///
    /// Example:
    /// ```rust
    /// let paths = vec!["some/dir/path/on/the/remote/cluster/".to_string()];
    ///
    /// // returns a DataFrame from a csv file with a header from a the specific path
    /// let mut df = spark.read().format("csv").option("header", "true").load(paths);
    /// ```
    pub fn load(&mut self, paths: Vec<String>) -> DataFrame {
        let read_type = Some(spark::relation::RelType::Read(spark::Read {
            is_streaming: false,
            read_type: Some(spark::read::ReadType::DataSource(spark::read::DataSource {
                format: self.format.clone(),
                schema: None,
                options: self.read_options.clone(),
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

        let logical_plan = LogicalPlanBuilder::new(relation);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }

    /// Returns the specific table as a [DataFrame]
    ///
    /// # Arguments:
    /// * `table_name`: &str of the table name
    /// * `options`: (optional Hashmap) contains additional read options for a table
    ///
    pub fn table(
        &mut self,
        table_name: &str,
        options: Option<HashMap<String, String>>,
    ) -> DataFrame {
        let read_type = Some(spark::relation::RelType::Read(spark::Read {
            is_streaming: false,
            read_type: Some(spark::read::ReadType::NamedTable(spark::read::NamedTable {
                unparsed_identifier: table_name.to_string(),
                options: options.unwrap_or(self.read_options.clone()),
            })),
        }));

        let relation = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(1),
            }),
            rel_type: read_type,
        };

        let logical_plan = LogicalPlanBuilder::new(relation);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }
}

/// DataFrameWriter provides the ability to output a [DataFrame]
/// to a specific file format supported by Spark
pub struct DataFrameWriter {
    dataframe: DataFrame,
    format: Option<String>,
    mode: SaveMode,
    bucket_by: Option<spark::write_operation::BucketBy>,
    partition_by: Vec<String>,
    sort_by: Vec<String>,
    write_options: HashMap<String, String>,
}

impl DataFrameWriter {
    /// Create a new DataFrameWriter from a provided [DataFrame]
    ///
    /// # Defaults
    /// - `format`: None,
    /// - `mode`: [SaveMode::Overwrite],
    /// - `bucket_by`: None,
    /// - `partition_by`: vec![],
    /// - `sort_by`: vec![],
    /// - `write_options`: HashMap::new()
    ///
    pub fn new(dataframe: DataFrame) -> Self {
        Self {
            dataframe,
            format: None,
            mode: SaveMode::Overwrite,
            bucket_by: None,
            partition_by: vec![],
            sort_by: vec![],
            write_options: HashMap::new(),
        }
    }

    /// Target format to output the [DataFrame]
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Specifies the behavior when data or table already exists
    ///
    /// # Arguments:
    /// - `mode`: (&str) translates to a specific [SaveMode] from the protobuf
    ///
    pub fn mode(mut self, mode: &str) -> Self {
        self.mode = match mode {
            "append" => SaveMode::Append,
            "overwrite" => SaveMode::Overwrite,
            "error" | "errorifexists" => SaveMode::ErrorIfExists,
            "ignore" => SaveMode::Ignore,
            _ => SaveMode::Unspecified,
        };
        self
    }

    /// Buckets the output by the given columns.
    /// If specified, the output is laid out on the file system
    /// similar to Hive’s bucketing scheme.
    #[allow(non_snake_case)]
    pub fn bucketBy(mut self, num_buckets: i32, buckets: Vec<String>) -> Self {
        self.bucket_by = Some(spark::write_operation::BucketBy {
            bucket_column_names: buckets,
            num_buckets,
        });
        self
    }

    /// Sorts the output in each bucket by the given columns on the file system
    #[allow(non_snake_case)]
    pub fn sortBy(mut self, cols: Vec<String>) -> Self {
        self.sort_by = cols;
        self
    }

    /// Partitions the output by the given columns on the file system
    #[allow(non_snake_case)]
    pub fn partitionBy(mut self, cols: Vec<String>) -> Self {
        self.sort_by = cols;
        self
    }

    /// Add an input option for the underlying data source
    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.write_options
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Set many input options based on a [HashMap] for the underlying data source
    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.write_options = options;
        self
    }

    /// Save the contents of the [DataFrame] to a data source.
    ///
    /// The data source is specified by the `format` and a set of `options`.
    pub async fn save(&mut self, path: &str) -> Result<(), ArrowError> {
        let write_command = spark::command::CommandType::WriteOperation(spark::WriteOperation {
            input: Some(self.dataframe.logical_plan.relation.clone()),
            source: self.format.clone(),
            mode: self.mode.into(),
            sort_column_names: self.sort_by.clone(),
            partitioning_columns: self.partition_by.clone(),
            bucket_by: self.bucket_by.clone(),
            options: self.write_options.clone(),
            save_type: Some(spark::write_operation::SaveType::Path(path.to_string())),
        });

        let plan = LogicalPlanBuilder::build_plan_cmd(write_command);

        self.dataframe
            .spark_session
            .consume_plan(Some(plan))
            .await
            .unwrap();

        Ok(())
    }

    async fn save_table(&mut self, table_name: &str, save_method: i32) -> Result<(), ArrowError> {
        let write_command = spark::command::CommandType::WriteOperation(spark::WriteOperation {
            input: Some(self.dataframe.logical_plan.relation.clone()),
            source: self.format.clone(),
            mode: self.mode.into(),
            sort_column_names: self.sort_by.clone(),
            partitioning_columns: self.partition_by.clone(),
            bucket_by: self.bucket_by.clone(),
            options: self.write_options.clone(),
            save_type: Some(spark::write_operation::SaveType::Table(
                spark::write_operation::SaveTable {
                    table_name: table_name.to_string(),
                    save_method,
                },
            )),
        });

        let plan = LogicalPlanBuilder::build_plan_cmd(write_command);

        self.dataframe
            .spark_session
            .consume_plan(Some(plan))
            .await
            .unwrap();

        Ok(())
    }

    /// Saves the context of the [DataFrame] as the specified table.
    #[allow(non_snake_case)]
    pub async fn saveAsTable(&mut self, table_name: &str) -> Result<(), ArrowError> {
        self.save_table(table_name, 1).await
    }

    /// Inserts the content of the [DataFrame] to the specified table.
    ///
    /// It requires that the schema of the [DataFrame] is the same as the
    /// schema of the target table.
    ///
    /// Unlike `saveAsTable()`, this method ignores the column names and just uses
    /// position-based resolution
    #[allow(non_snake_case)]
    pub async fn insertInto(&mut self, table_name: &str) -> Result<(), ArrowError> {
        self.save_table(table_name, 2).await
    }
}
