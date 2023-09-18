//! DataFrame with Reader/Writer repesentation

use std::collections::HashMap;

use crate::execution;
use crate::plan::LogicalPlanBuilder;
use crate::spark;

use spark::expression::{ExprType, ExpressionString};
use spark::relation::RelType;
use spark::write_operation::SaveMode;
use spark::Expression;

use execution::context::SparkSession;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

/// DataFrame is composed of a `spark_session` connecting to a remote
/// Spark Connect enabled cluster, and a `logical_plan` which represents
/// the `Plan` to be submitted to the cluster when an action is called
#[derive(Clone, Debug)]
pub struct DataFrame {
    /// Global [SparkSession] connecting to the remote cluster
    pub spark_session: SparkSession,

    /// Logical Plan representing the unresolved Relation
    /// which will be submitted to the remote cluster
    pub logical_plan: LogicalPlanBuilder,
}

impl DataFrame {
    /// create default DataFrame based on a spark session and initial logical plan
    pub fn new(spark_session: SparkSession, logical_plan: LogicalPlanBuilder) -> DataFrame {
        DataFrame {
            spark_session,
            logical_plan,
        }
    }

    /// Projects a set of expressions and returns a new [DataFrame]
    ///
    /// # Arguments:
    ///
    /// * `cols` is a vector of `&str` which resolve to a specific column
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.select(vec!["age", "name"]).collect().await?;
    /// }
    /// ```
    pub fn select(&mut self, cols: Vec<&str>) -> DataFrame {
        let expressions: Vec<spark::Expression> = cols
            .iter()
            .map(|&col| spark::Expression {
                expr_type: Some(spark::expression::ExprType::UnresolvedAttribute(
                    spark::expression::UnresolvedAttribute {
                        unparsed_identifier: col.to_string(),
                        plan_id: None,
                    },
                )),
            })
            .collect();

        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions,
            input: Some(Box::new(self.logical_plan.clone().relation)),
        }));

        let logical_plan = self.logical_plan.from(rel_type);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }

    /// Project a set of SQL expressions and returns a new [DataFrame]
    ///
    /// This is a variant of `select` that accepts SQL Expressions
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.selectExpr(vec!["id * 2", "abs(id)"]).collect().await?;
    /// }
    /// ```
    #[allow(non_snake_case)]
    pub fn selectExpr(&mut self, cols: Vec<&str>) -> DataFrame {
        let expressions: Vec<spark::Expression> = cols
            .iter()
            .map(|&col| spark::Expression {
                expr_type: Some(spark::expression::ExprType::ExpressionString(
                    spark::expression::ExpressionString {
                        expression: col.to_string(),
                    },
                )),
            })
            .collect();

        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions,
            input: Some(Box::new(self.logical_plan.clone().relation)),
        }));

        let logical_plan = self.logical_plan.from(rel_type);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }

    /// Filters rows using a given conditions and returns a new [DataFrame]
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.filter("salary > 4000").collect().await?;
    /// }
    /// ```
    pub fn filter(&mut self, condition: &str) -> DataFrame {
        let filter_expr = ExprType::ExpressionString(ExpressionString {
            expression: condition.to_string(),
        });

        let rel_type = RelType::Filter(Box::new(spark::Filter {
            input: Some(Box::new(self.logical_plan.clone().relation)),
            condition: Some(Expression {
                expr_type: Some(filter_expr),
            }),
        }));
        let logical_plan = self.logical_plan.from(rel_type);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }

    /// Limits the result count o thte number specified and returns a new [DataFrame]
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.limit(10).collect().await?;
    /// }
    /// ```
    pub fn limit(&mut self, limit: i32) -> DataFrame {
        let limit_expr = RelType::Limit(Box::new(spark::Limit {
            input: Some(Box::new(self.logical_plan.clone().relation)),
            limit,
        }));

        let logical_plan = self.logical_plan.from(limit_expr);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }

    /// Returns a new [DataFrame] partitioned by the given partition number and shuffle
    /// option
    ///
    /// # Arguments
    ///
    /// * `num_partitions`: the target number of partitions
    /// * (optional) `shuffle`: to induce a shuffle. Default is `false`
    ///
    pub fn repartition(&mut self, num_partitions: i32, shuffle: Option<bool>) -> DataFrame {
        let repart_expr = RelType::Repartition(Box::new(spark::Repartition {
            input: Some(Box::new(self.logical_plan.clone().relation)),
            num_partitions,
            shuffle,
        }));

        let logical_plan = self.logical_plan.from(repart_expr);

        DataFrame::new(self.spark_session.clone(), logical_plan)
    }

    #[allow(non_snake_case, dead_code)]
    async fn createTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), false, false)
            .await
            .unwrap()
    }

    #[allow(non_snake_case, dead_code)]
    async fn createGlobalTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), true, false)
            .await
            .unwrap()
    }

    #[allow(non_snake_case, dead_code)]
    async fn createOrReplaceGlobalTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), true, true)
            .await
            .unwrap()
    }

    #[allow(non_snake_case, dead_code)]
    async fn createOrReplaceTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), false, true)
            .await
            .unwrap()
    }

    async fn create_view_cmd(
        &mut self,
        name: String,
        is_global: bool,
        replace: bool,
    ) -> Result<(), ArrowError> {
        let command_type =
            spark::command::CommandType::CreateDataframeView(spark::CreateDataFrameViewCommand {
                input: Some(self.logical_plan.relation.clone()),
                name,
                is_global,
                replace,
            });

        let plan = self.logical_plan.clone().build_plan_cmd(command_type);

        self.spark_session.consume_plan(Some(plan)).await?;

        Ok(())
    }

    /// Prints the first `n` rows to the console
    ///
    /// # Arguments:
    ///
    /// * `num_row`: (int, optional) number of rows to show (default 10)
    /// * `truncate`: (int, optional) If set to 0, it truncates the string. Any other number will not truncate the strings
    /// * `vertical`: (bool, optional) If set to true, prints output rows vertically (one line per column value).
    ///
    pub async fn show(
        &mut self,
        num_rows: Option<i32>,
        truncate: Option<i32>,
        vertical: Option<bool>,
    ) -> Result<(), ArrowError> {
        let show_expr = RelType::ShowString(Box::new(spark::ShowString {
            input: Some(Box::new(self.logical_plan.clone().relation)),
            num_rows: num_rows.unwrap_or(10),
            truncate: truncate.unwrap_or(0),
            vertical: vertical.unwrap_or(false),
        }));

        let plan = self.logical_plan.from(show_expr).build_plan_root();

        let rows = self.spark_session.consume_plan(Some(plan)).await.unwrap();

        let _ = pretty::print_batches(rows.as_slice());
        Ok(())
    }

    /// Returns the last `n` rows as vector of [RecordBatch]
    ///
    /// Running tail requires moving the data and results in an action
    ///
    pub async fn tail(&mut self, limit: i32) -> Result<Vec<RecordBatch>, ArrowError> {
        let limit_expr = RelType::Tail(Box::new(spark::Tail {
            input: Some(Box::new(self.logical_plan.clone().relation)),
            limit,
        }));

        let plan = self.logical_plan.from(limit_expr).build_plan_root();

        let rows = self.spark_session.consume_plan(Some(plan)).await.unwrap();

        Ok(rows)
    }

    /// Returns all records as a vector of [RecordBatch]
    ///
    /// # Example:
    ///
    /// ```rust
    /// async {
    ///     df.collect().await?;
    /// }
    /// ```
    pub async fn collect(&mut self) -> Result<Vec<RecordBatch>, ArrowError> {
        let rows = self
            .spark_session
            .consume_plan(Some(self.logical_plan.clone().build_plan_root()))
            .await
            .unwrap();

        Ok(rows)
    }

    /// Returns a [DataFrameWriter] struct based on the current [DataFrame]
    pub fn write(self) -> DataFrameWriter {
        DataFrameWriter::new(self)
    }
}

/// DataFrameReader represents the entrypoint to create a DataFrame
/// from a specific file format.
#[derive(Clone, Debug)]
pub struct DataFrameReader {
    spark_session: SparkSession,
    format: Option<String>,
    read_options: HashMap<String, String>,
}

impl DataFrameReader {
    /// Create a new DataFraemReader with a [SparkSession]
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

        let plan = self
            .dataframe
            .logical_plan
            .clone()
            .build_plan_cmd(write_command);

        self.dataframe
            .spark_session
            .consume_plan(Some(plan))
            .await
            .unwrap();

        Ok(())
    }
}
