//! DataFrame representation for Spark Connection

use std::sync::Arc;

use crate::column::Column;
use crate::errors::SparkError;
use crate::expressions::{ToExpr, ToFilterExpr, ToVecExpr};
use crate::group::GroupedData;
use crate::plan::LogicalPlanBuilder;
pub use crate::readwriter::{DataFrameReader, DataFrameWriter};
use crate::session::SparkSession;
use crate::spark;
use crate::storage;
pub use crate::streaming::{DataStreamReader, DataStreamWriter, OutputMode, StreamingQuery};
pub use spark::aggregate::GroupType;
pub use spark::analyze_plan_request::explain::ExplainMode;
pub use spark::join::JoinType;
pub use spark::write_operation::SaveMode;

use spark::relation::RelType;

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

/// DataFrame is composed of a [SparkSession] referencing a
/// Spark Connect enabled cluster, and a [LogicalPlanBuilder] which represents
/// the unresolved [spark::Plan] to be submitted to the cluster when an action is called.
///
/// The [LogicalPlanBuilder] is a series of unresolved logical plans, and every additional
/// transformation takes the prior [spark::Plan] and builds onto it. The final unresolved logical
/// plan is submitted to the spark connect server.
///
/// ## createDataFrame & range
///
/// A `DataFrame` can be created with an [arrow::array::RecordBatch], or with `spark.range(...)`
///
/// ```rust
/// let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
/// let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
///
/// let data = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?
///
/// let df = spark.createDataFrame(&data).await?
/// ```
///
/// ## sql
///
/// A `DataFrame` is created from a `spark.sql()` statement
///
/// ```rust
/// let df = spark.sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`").await?;
/// ```
///
/// ## read & readStream
///
/// A `DataFrame` is also created from a `spark.read()` and `spark.readStream()` statement.
///
/// ```rust
/// let df = spark
///     .read()
///     .format("csv")
///     .option("header", "True")
///     .option("delimiter", ";")
///     .load(paths)?;
/// ````
#[derive(Clone, Debug)]
pub struct DataFrame {
    /// Global [SparkSession] connecting to the remote cluster
    pub spark_session: Arc<SparkSession>,

    /// Logical Plan representing the unresolved Relation
    /// which will be submitted to the remote cluster
    pub logical_plan: LogicalPlanBuilder,
}

impl DataFrame {
    /// create default DataFrame based on a spark session and initial logical plan
    pub fn new(spark_session: Arc<SparkSession>, logical_plan: LogicalPlanBuilder) -> DataFrame {
        DataFrame {
            spark_session,
            logical_plan,
        }
    }

    fn check_same_session(&self, other: &DataFrame) -> Result<(), SparkError> {
        if self.spark_session.session_id() != other.spark_session.session_id() {
            return Err(SparkError::AnalysisException(
                "Spark Session is not the same!".to_string(),
            ));
        };

        Ok(())
    }

    /// Aggregate on the entire [DataFrame] without groups (shorthand for `df.groupBy().agg()`)
    pub fn agg<T: ToVecExpr>(self, exprs: T) -> DataFrame {
        self.groupBy::<Column>(None).agg(exprs)
    }

    /// Returns a new [DataFrame] with an alias set.
    pub fn alias(self, alias: &str) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.alias(alias))
    }

    /// Persists the [DataFrame] with the default [storage::StorageLevel::MemoryAndDiskDeser] (MEMORY_AND_DISK_DESER).
    pub async fn cache(self) -> DataFrame {
        self.persist(storage::StorageLevel::MemoryAndDiskDeser)
            .await
    }

    /// Returns a new [DataFrame] that has exactly `num_partitions` partitions.
    pub fn coalesce(self, num_partitions: u32) -> DataFrame {
        self.repartition(num_partitions, Some(false))
    }

    /// Returns the number of rows in this [DataFrame]
    pub async fn count(self) -> Result<i64, SparkError> {
        let res = self.groupBy::<Column>(None).count().collect().await?;
        let col = res.column(0);

        let data: &arrow::array::Int64Array = match col.data_type() {
            arrow::datatypes::DataType::Int64 => col.as_any().downcast_ref().unwrap(),
            _ => unimplemented!("only Utf8 data types are currently handled currently."),
        };

        Ok(data.value(0))
    }

    /// Selects column based on the column name specified as a regex and returns it as [Column].
    #[allow(non_snake_case)]
    pub fn colRegex(self, col_name: &str) -> Column {
        let expr = spark::Expression {
            expr_type: Some(spark::expression::ExprType::UnresolvedRegex(
                spark::expression::UnresolvedRegex {
                    col_name: col_name.to_string(),
                    plan_id: Some(self.logical_plan.plan_id()),
                },
            )),
        };
        Column::from(expr)
    }

    /// Returns all records as a [RecordBatch]
    ///
    /// # Example:
    ///
    /// ```rust
    /// async {
    ///     df.collect().await?;
    /// }
    /// ```
    pub async fn collect(self) -> Result<RecordBatch, SparkError> {
        let plan = LogicalPlanBuilder::plan_root(self.logical_plan);
        self.spark_session.client().to_arrow(plan).await
    }

    /// Retrieves the names of all columns in the [DataFrame] as a `Vec<String>`.
    /// The order of the column names in the list reflects their order in the [DataFrame].
    pub async fn columns(self) -> Result<Vec<String>, SparkError> {
        let schema = self.schema().await?;

        let struct_val = schema.kind.expect("Unwrapped an empty schema");

        let cols = match struct_val {
            spark::data_type::Kind::Struct(val) => val
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect(),
            _ => unimplemented!("Unexpected schema response"),
        };

        Ok(cols)
    }
    /// Calculates the correlation of two columns of a [DataFrame] as a `f64`.
    /// Currently only supports the Pearson Correlation Coefficient.
    pub async fn corr(self, col1: &str, col2: &str) -> Result<f64, SparkError> {
        let result = DataFrame::new(self.spark_session, self.logical_plan.corr(col1, col2))
            .collect()
            .await?;

        let col = result.column(0);

        let data: &PrimitiveArray<Float64Type> = match col.data_type() {
            DataType::Float64 => col
                .as_any()
                .downcast_ref()
                .expect("failed to unwrap result"),
            _ => panic!("Expected Float64 in response type"),
        };

        Ok(data.value(0))
    }

    /// Calculate the sample covariance for the given columns, specified by their names, as a f64
    pub async fn cov(self, col1: &str, col2: &str) -> Result<f64, SparkError> {
        let result = DataFrame::new(self.spark_session, self.logical_plan.cov(col1, col2))
            .collect()
            .await?;

        let col = result.column(0);

        let data: &PrimitiveArray<Float64Type> = match col.data_type() {
            DataType::Float64 => col
                .as_any()
                .downcast_ref()
                .expect("failed to unwrap result"),
            _ => panic!("Expected Float64 in response type"),
        };

        Ok(data.value(0))
    }

    /// Creates a local temporary view with this DataFrame.
    #[allow(non_snake_case)]
    pub async fn createTempView(self, name: &str) -> Result<(), SparkError> {
        self.create_view_cmd(name, false, false).await
    }

    #[allow(non_snake_case)]
    pub async fn createGlobalTempView(self, name: &str) -> Result<(), SparkError> {
        self.create_view_cmd(name, true, false).await
    }

    #[allow(non_snake_case)]
    pub async fn createOrReplaceGlobalTempView(self, name: &str) -> Result<(), SparkError> {
        self.create_view_cmd(name, true, true).await
    }

    /// Creates or replaces a local temporary view with this DataFrame
    #[allow(non_snake_case)]
    pub async fn createOrReplaceTempView(self, name: &str) -> Result<(), SparkError> {
        self.create_view_cmd(name, false, true).await
    }

    async fn create_view_cmd(
        self,
        name: &str,
        is_global: bool,
        replace: bool,
    ) -> Result<(), SparkError> {
        let command_type =
            spark::command::CommandType::CreateDataframeView(spark::CreateDataFrameViewCommand {
                input: Some(self.logical_plan.relation()),
                name: name.to_string(),
                is_global,
                replace,
            });

        let plan = LogicalPlanBuilder::plan_cmd(command_type);

        self.spark_session.client().execute_command(plan).await?;
        Ok(())
    }

    /// Returns the cartesian product with another [DataFrame].
    #[allow(non_snake_case)]
    pub fn crossJoin(self, other: DataFrame) -> DataFrame {
        DataFrame::new(
            self.spark_session,
            self.logical_plan
                .join(other.logical_plan, None::<&str>, JoinType::Cross, vec![]),
        )
    }

    /// Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
    pub fn crosstab(self, col1: &str, col2: &str) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.crosstab(col1, col2))
    }

    /// Create a multi-dimensional cube for the current DataFrame using the specified columns, so we can run aggregations on them.
    pub fn cube<T: ToVecExpr>(self, cols: T) -> GroupedData {
        GroupedData::new(self, GroupType::Cube, cols.to_vec_expr(), None, None)
    }

    // Computes basic statistics for numeric and string columns. This includes count, mean, stddev, min, and max.
    // If no columns are given, this function computes statistics for all numerical or string columns.
    pub fn describe<'a, I>(self, cols: Option<I>) -> DataFrame
    where
        I: IntoIterator<Item = &'a str> + std::default::Default,
    {
        DataFrame::new(self.spark_session, self.logical_plan.describe(cols))
    }

    /// Returns a new [DataFrame] containing the distinct rows in this [DataFrame].
    pub fn distinct(self) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.distinct())
    }

    /// Returns a new [DataFrame] without the specified columns
    pub fn drop<T: ToVecExpr>(self, cols: T) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.drop(cols))
    }

    /// Return a new [DataFrame] with duplicate rows removed,
    /// optionally only considering certain columns from a `Vec<String>`
    ///
    /// If no columns are supplied then it all columns are used
    ///
    /// Alias for `dropDuplciates`
    ///
    pub fn drop_duplicates(self, cols: Option<Vec<&str>>) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.drop_duplicates(cols))
    }

    /// Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.
    #[allow(non_snake_case)]
    pub fn dropDuplicates(self, cols: Option<Vec<&str>>) -> DataFrame {
        self.drop_duplicates(cols)
    }

    /// Returns a new DataFrame omitting rows with null values.
    pub fn dropna(self, how: &str, threshold: Option<i32>, subset: Option<Vec<&str>>) -> DataFrame {
        DataFrame::new(
            self.spark_session,
            self.logical_plan.dropna(how, threshold, subset),
        )
    }

    /// Returns all column names and their data types as a Vec containing
    /// the field name as a String and the [spark::data_type::Kind] enum
    pub async fn dtypes(self) -> Result<Vec<(String, spark::data_type::Kind)>, SparkError> {
        let schema = self.schema().await?;

        let struct_val = schema.kind.expect("unwrapped an empty schema");

        let dtypes = match struct_val {
            spark::data_type::Kind::Struct(val) => val
                .fields
                .iter()
                .map(|field| {
                    (
                        field.name.to_string(),
                        field.data_type.clone().unwrap().kind.unwrap(),
                    )
                })
                .collect(),
            _ => unimplemented!("Unexpected schema response"),
        };

        Ok(dtypes)
    }

    /// Return a new DataFrame containing rows in this DataFrame but not in another DataFrame while preserving duplicates.
    #[allow(non_snake_case)]
    pub fn exceptAll(self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan.exceptAll(other.logical_plan),
        )
    }

    /// Prints the [spark::Plan] to the console
    ///
    /// # Arguments:
    /// * `mode`: [ExplainMode] Defaults to `unspecified`
    ///     - `simple`
    ///     - `extended`
    ///     - `codegen`
    ///     - `cost`
    ///     - `formatted`
    ///     - `unspecified`
    ///
    pub async fn explain(self, mode: Option<ExplainMode>) -> Result<String, SparkError> {
        let explain_mode = match mode {
            Some(mode) => mode,
            None => ExplainMode::Simple,
        };

        let plan = LogicalPlanBuilder::plan_root(self.logical_plan);

        let analyze =
            spark::analyze_plan_request::Analyze::Explain(spark::analyze_plan_request::Explain {
                plan: Some(plan),
                explain_mode: explain_mode.into(),
            });

        let explain = self
            .spark_session
            .client()
            .analyze(analyze)
            .await?
            .explain()?;

        println!("{}", explain);

        Ok(explain)
    }

    /// Filters rows using a given conditions and returns a new [DataFrame]
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.filter("salary > 4000").collect().await?;
    /// }
    /// ```
    pub fn filter<T: ToFilterExpr>(self, condition: T) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.filter(condition))
    }

    /// Returns the first row as a RecordBatch.
    pub async fn first(self) -> Result<RecordBatch, SparkError> {
        self.head(None).await
    }

    /// Finding frequent items for columns, possibly with false positives.
    #[allow(non_snake_case)]
    pub fn freqItems<'a, I>(self, cols: I, support: Option<f64>) -> DataFrame
    where
        I: IntoIterator<Item = &'a str>,
    {
        DataFrame::new(
            self.spark_session,
            self.logical_plan.freqItems(cols, support),
        )
    }

    /// Groups the DataFrame using the specified columns, and returns a [GroupedData] object
    #[allow(non_snake_case)]
    pub fn groupBy<T: ToVecExpr>(self, cols: Option<T>) -> GroupedData {
        let grouping_cols = match cols {
            Some(cols) => cols.to_vec_expr(),
            None => vec![],
        };
        GroupedData::new(self, GroupType::Groupby, grouping_cols, None, None)
    }

    /// Returns the first n rows.
    pub async fn head(self, n: Option<i32>) -> Result<RecordBatch, SparkError> {
        self.limit(n.unwrap_or(1)).collect().await
    }

    /// Specifies some hint on the current DataFrame.
    pub fn hint<T: ToVecExpr>(self, name: &str, parameters: Option<T>) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.hint(name, parameters))
    }

    /// Returns a best-effort snapshot of the files that compose this DataFrame
    #[allow(non_snake_case)]
    pub async fn inputFiles(self) -> Result<Vec<String>, SparkError> {
        let input_files = spark::analyze_plan_request::Analyze::InputFiles(
            spark::analyze_plan_request::InputFiles {
                plan: Some(LogicalPlanBuilder::plan_root(self.logical_plan)),
            },
        );

        self.spark_session
            .client()
            .analyze(input_files)
            .await?
            .input_files()
    }

    /// Return a new DataFrame containing rows only in both this DataFrame and another DataFrame.
    pub fn intersect(self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan.intersect(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub fn intersectAll(self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan.intersectAll(other.logical_plan),
        )
    }

    /// Checks if the DataFrame is empty and returns a boolean value.
    #[allow(non_snake_case)]
    pub async fn isEmpty(self) -> Result<bool, SparkError> {
        let val = &self.select("*").limit(1).collect().await?;

        Ok(val.num_rows() == 0)
    }

    /// Returns True if this DataFrame contains one or more sources that continuously return data as it arrives.
    #[allow(non_snake_case)]
    pub async fn isStreaming(self) -> Result<bool, SparkError> {
        let is_streaming = spark::analyze_plan_request::Analyze::IsStreaming(
            spark::analyze_plan_request::IsStreaming {
                plan: Some(LogicalPlanBuilder::plan_root(self.logical_plan)),
            },
        );

        self.spark_session
            .client()
            .analyze(is_streaming)
            .await?
            .is_streaming()
    }

    /// Joins with another DataFrame, using the given join expression.
    ///
    /// # Example:
    /// ```rust
    /// use spark_connect_rs::functions::col;
    /// use spark_connect_rs::dataframe::JoinType;
    ///
    /// async {
    ///     // join two dataframes where `id` == `name`
    ///     let condition = Some(col("id").eq(col("name")));
    ///     let df = df.join(df2, condition, JoinType::Inner);
    /// }
    /// ```

    pub fn join<T: ToExpr>(self, other: DataFrame, on: Option<T>, how: JoinType) -> DataFrame {
        DataFrame::new(
            self.spark_session,
            self.logical_plan.join(other.logical_plan, on, how, vec![]),
        )
    }

    /// Limits the result count o thte number specified and returns a new [DataFrame]
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.limit(10).collect().await?;
    /// }
    /// ```
    pub fn limit(self, limit: i32) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.limit(limit))
    }

    /// Alias for [DataFrame::unpivot]
    pub fn melt<I, K>(
        self,
        ids: I,
        values: Option<K>,
        variable_column_name: &str,
        value_column_name: &str,
    ) -> DataFrame
    where
        I: ToVecExpr,
        K: ToVecExpr,
    {
        self.unpivot(ids, values, variable_column_name, value_column_name)
    }

    /// Returns a new [DataFrame] by skiping the first n rows
    pub fn offset(self, num: i32) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.offset(num))
    }

    #[allow(non_snake_case)]
    pub fn orderBy<I>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = Column>,
    {
        DataFrame::new(self.spark_session, self.logical_plan.sort(cols))
    }

    pub async fn persist(self, storage_level: storage::StorageLevel) -> DataFrame {
        let analyze =
            spark::analyze_plan_request::Analyze::Persist(spark::analyze_plan_request::Persist {
                relation: Some(self.logical_plan.clone().relation()),
                storage_level: Some(storage_level.into()),
            });

        self.spark_session
            .clone()
            .client()
            .analyze(analyze)
            .await
            .unwrap();

        DataFrame::new(self.spark_session, self.logical_plan)
    }

    /// Prints out the schema in the tree format to a specific level number.
    #[allow(non_snake_case)]
    pub async fn printSchema(self, level: Option<i32>) -> Result<String, SparkError> {
        let tree_string = spark::analyze_plan_request::Analyze::TreeString(
            spark::analyze_plan_request::TreeString {
                plan: Some(LogicalPlanBuilder::plan_root(self.logical_plan)),
                level,
            },
        );
        let tree = self
            .spark_session
            .client()
            .analyze(tree_string)
            .await?
            .tree_string()?;

        Ok(tree)
    }

    /// Returns a new [DataFrame] partitioned by the given partition number and shuffle option
    ///
    /// # Arguments
    ///
    /// * `num_partitions`: the target number of partitions
    /// * (optional) `shuffle`: to induce a shuffle. Default is `false`
    ///
    pub fn repartition(self, num_partitions: u32, shuffle: Option<bool>) -> DataFrame {
        DataFrame::new(
            self.spark_session,
            self.logical_plan.repartition(num_partitions, shuffle),
        )
    }

    /// Create a multi-dimensional rollup for the current DataFrame using the specified columns,
    /// and returns a [GroupedData] object
    pub fn rollup<T: ToVecExpr>(self, cols: T) -> GroupedData {
        GroupedData::new(self, GroupType::Rollup, cols.to_vec_expr(), None, None)
    }

    /// Returns True when the logical query plans inside both DataFrames are equal and therefore return the same results.
    #[allow(non_snake_case)]
    pub async fn sameSemantics(self, other: DataFrame) -> Result<bool, SparkError> {
        let target_plan = Some(LogicalPlanBuilder::plan_root(self.logical_plan));
        let other_plan = Some(LogicalPlanBuilder::plan_root(other.logical_plan));

        let same_semantics = spark::analyze_plan_request::Analyze::SameSemantics(
            spark::analyze_plan_request::SameSemantics {
                target_plan,
                other_plan,
            },
        );
        let same_semantics = self
            .spark_session
            .client()
            .analyze(same_semantics)
            .await?
            .same_semantics()?;

        Ok(same_semantics)
    }

    /// Returns a sampled subset of this [DataFrame]
    pub fn sample(
        self,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: Option<bool>,
        seed: Option<i64>,
    ) -> DataFrame {
        DataFrame::new(
            self.spark_session,
            self.logical_plan
                .sample(lower_bound, upper_bound, with_replacement, seed),
        )
    }

    /// Returns the schema of this DataFrame as a [spark::DataType]
    /// which contains the schema of a DataFrame
    pub async fn schema(self) -> Result<spark::DataType, SparkError> {
        let plan = LogicalPlanBuilder::plan_root(self.logical_plan);

        let schema =
            spark::analyze_plan_request::Analyze::Schema(spark::analyze_plan_request::Schema {
                plan: Some(plan),
            });
        let session = self.spark_session.clone();
        let mut client = session.client();
        let data_type = client.analyze(schema).await?;
        let schema = data_type.schema()?;
        Ok(schema.clone())
    }

    /// Projects a set of expressions and returns a new [DataFrame]
    ///
    /// # Arguments:
    ///
    /// * `cols` - An object that implements [ToVecExpr]
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.select(vec![col("age"), col("name")]).collect().await?;
    /// }
    /// ```
    pub fn select<T: ToVecExpr>(self, cols: T) -> DataFrame {
        DataFrame::new(self.spark_session, self.logical_plan.select(cols))
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
    pub fn selectExpr<'a, I>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = &'a str>,
    {
        DataFrame::new(self.spark_session, self.logical_plan.select_expr(cols))
    }

    #[allow(non_snake_case)]
    pub async fn semanticHash(self) -> Result<i32, SparkError> {
        let plan = LogicalPlanBuilder::plan_root(self.logical_plan);

        let semantic_hash = spark::analyze_plan_request::Analyze::SemanticHash(
            spark::analyze_plan_request::SemanticHash { plan: Some(plan) },
        );

        let semantic_hash = self
            .spark_session
            .client()
            .analyze(semantic_hash)
            .await?
            .semantic_hash()?;

        Ok(semantic_hash)
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
        self,
        num_rows: Option<i32>,
        truncate: Option<i32>,
        vertical: Option<bool>,
    ) -> Result<(), SparkError> {
        let show_expr = RelType::ShowString(Box::new(spark::ShowString {
            input: self.logical_plan.relation_input(),
            num_rows: num_rows.unwrap_or(10),
            truncate: truncate.unwrap_or(0),
            vertical: vertical.unwrap_or(false),
        }));

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(show_expr));

        let rows = self.spark_session.client().to_arrow(plan).await?;

        Ok(pretty::print_batches(&[rows])?)
    }

    pub fn sort<I>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = Column>,
    {
        DataFrame::new(self.spark_session, self.logical_plan.sort(cols))
    }

    #[allow(non_snake_case)]
    pub fn sparkSession(self) -> Arc<SparkSession> {
        self.spark_session
    }

    #[allow(non_snake_case)]
    pub async fn storageLevel(self) -> Result<storage::StorageLevel, SparkError> {
        let storage_level = spark::analyze_plan_request::Analyze::GetStorageLevel(
            spark::analyze_plan_request::GetStorageLevel {
                relation: Some(self.logical_plan.relation()),
            },
        );

        let storage = self
            .spark_session
            .client()
            .analyze(storage_level)
            .await?
            .get_storage_level()?;

        Ok(storage.into())
    }

    pub fn subtract(self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan.substract(other.logical_plan),
        )
    }

    /// Returns the last `n` rows as a [RecordBatch]
    ///
    /// Running tail requires moving the data and results in an action
    ///
    pub async fn tail(self, limit: i32) -> Result<RecordBatch, SparkError> {
        let limit_expr = RelType::Tail(Box::new(spark::Tail {
            input: self.logical_plan.relation_input(),
            limit,
        }));

        let logical_plan = LogicalPlanBuilder::from(limit_expr);

        DataFrame::new(self.spark_session, logical_plan)
            .collect()
            .await
    }

    pub async fn take(self, n: i32) -> Result<RecordBatch, SparkError> {
        self.limit(n).collect().await
    }

    #[allow(non_snake_case)]
    pub fn toDF<'a, I>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = &'a str>,
    {
        DataFrame::new(self.spark_session, self.logical_plan.to_df(cols))
    }

    pub fn union(self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan.unionAll(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub fn unionAll(self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan.unionAll(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub fn unionByName(self, other: DataFrame, allow_missing_columns: Option<bool>) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session,
            self.logical_plan
                .unionByName(other.logical_plan, allow_missing_columns),
        )
    }

    pub async fn unpersist(self, blocking: Option<bool>) -> DataFrame {
        let unpersist = spark::analyze_plan_request::Analyze::Unpersist(
            spark::analyze_plan_request::Unpersist {
                relation: Some(self.logical_plan.clone().relation()),
                blocking,
            },
        );

        self.spark_session
            .clone()
            .client()
            .analyze(unpersist)
            .await
            .unwrap();

        DataFrame::new(self.spark_session, self.logical_plan)
    }

    /// Unpivot a DataFrame from wide format to long format, optionally leaving identifier columns set.
    /// This is the reverse to groupBy(…).pivot(…).agg(…), except for the aggregation, which cannot be reversed.
    pub fn unpivot<I, K>(
        self,
        ids: I,
        values: Option<K>,
        variable_column_name: &str,
        value_column_name: &str,
    ) -> DataFrame
    where
        I: ToVecExpr,
        K: ToVecExpr,
    {
        let ids = ids.to_vec_expr();
        let values = values.map(|val| val.to_vec_expr());

        let logical_plan =
            self.logical_plan
                .unpivot(ids, values, variable_column_name, value_column_name);

        DataFrame::new(self.spark_session, logical_plan)
    }

    #[allow(non_snake_case)]
    pub fn withColumn(self, colName: &str, col: Column) -> DataFrame {
        DataFrame::new(
            self.spark_session,
            self.logical_plan.withColumn(colName, col),
        )
    }

    #[allow(non_snake_case)]
    pub fn withColumns<I, K>(self, colMap: I) -> DataFrame
    where
        I: IntoIterator<Item = (K, Column)>,
        K: ToString,
    {
        DataFrame::new(self.spark_session, self.logical_plan.withColumns(colMap))
    }

    /// Returns a new [DataFrame] by renaming multiple columns from a
    /// an iterator of containing a key/value pair with the key as the `existing`
    /// column name and the value as the `new` column name.
    #[allow(non_snake_case)]
    pub fn withColumnsRenamed<I, K, V>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        DataFrame::new(
            self.spark_session,
            self.logical_plan.withColumnsRenamed(cols),
        )
    }
    /// Returns a [DataFrameWriter] struct based on the current [DataFrame]
    pub fn write(self) -> DataFrameWriter {
        DataFrameWriter::new(self)
    }

    /// Interface for [DataStreamWriter] to save the content of the streaming DataFrame out
    /// into external storage.
    #[allow(non_snake_case)]
    pub fn writeStream(self) -> DataStreamWriter {
        DataStreamWriter::new(self)
    }
}

#[cfg(test)]
mod tests {

    use arrow::{
        array::{ArrayRef, Float32Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::{collections::HashMap, sync::Arc};

    use super::*;

    use crate::functions::*;
    use crate::SparkSessionBuilder;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection =
            "sc://127.0.0.1:15002/;user_id=rust_df;session_id=b5714cb4-6bb4-4c02-90b1-b9b93c70b323";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    fn mock_data() -> RecordBatch {
        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));

        RecordBatch::try_from_iter(vec![("name", name), ("age", age)]).unwrap()
    }

    #[tokio::test]
    async fn test_df_alias() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let df_as1 = df.clone().alias("df_as1");
        let df_as2 = df.alias("df_as2");

        let condition = Some(col("df_as1.name").eq(col("df_as2.name")));

        let joined_df = df_as1.join(df_as2, condition, JoinType::Inner);

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));

        let expected =
            RecordBatch::try_from_iter(vec![("name", name.clone()), ("name", name), ("age", age)])?;

        let res = joined_df
            .clone()
            .select(["df_as1.name", "df_as2.name", "df_as2.age"])
            .sort([asc("df_as1.name")])
            .collect()
            .await?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_cache() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 2, 1, None);
        df.clone().cache().await;

        let exp = df.clone().explain(None).await?;
        assert!(exp.contains("InMemoryTableScan"));
        Ok(())
    }

    #[tokio::test]
    async fn test_df_coalesce() -> Result<(), SparkError> {
        let spark = setup().await;

        // partition num of 5 would create 5 different values for
        // spark_partition_id
        let val = spark
            .range(None, 10, 1, Some(5))
            .coalesce(1)
            .select(spark_partition_id().alias("partition"))
            .distinct()
            .collect()
            .await?;

        assert_eq!(1, val.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn test_df_colregex() -> Result<(), SparkError> {
        let spark = setup().await;

        let col1: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let col2: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));

        let data = RecordBatch::try_from_iter(vec![("col1", col1), ("col2", col2)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .clone()
            .select(df.colRegex("`(Col1)?+.+`"))
            .columns()
            .await?;

        assert_eq!(vec!["col2".to_string(),], res);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_columns() -> Result<(), SparkError> {
        let spark = setup().await;

        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let state: ArrayRef = Arc::new(StringArray::from(vec!["CA", "NY", "TX"]));

        let data =
            RecordBatch::try_from_iter(vec![("age", age), ("name", name), ("state", state)])?;

        let df = spark.createDataFrame(&data)?;

        let cols = df.clone().columns().await?;

        assert_eq!(
            vec!["age".to_string(), "name".to_string(), "state".to_string()],
            cols
        );

        let select_cols: Vec<String> = cols.into_iter().filter(|c| c != "age").collect();

        let cols = df.select(select_cols.clone()).columns().await?;

        assert_eq!(select_cols, cols);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_corr() -> Result<(), SparkError> {
        let spark = setup().await;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 10, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![12, 1, 8]));

        let data = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        let val = spark.createDataFrame(&data)?.corr("c1", "c2").await?;

        assert_eq!(-0.3592106040535498_f64, val);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_count() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        assert_eq!(3, df.count().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_cov() -> Result<(), SparkError> {
        let spark = setup().await;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 10, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![12, 1, 8]));

        let data = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        let val = spark
            .clone()
            .createDataFrame(&data)?
            .cov("c1", "c2")
            .await?;

        assert_eq!(-18.0_f64, val);

        let small: ArrayRef = Arc::new(Int64Array::from(vec![11, 10, 9]));
        let big: ArrayRef = Arc::new(Int64Array::from(vec![12, 11, 10]));

        let data = RecordBatch::try_from_iter(vec![("small", small), ("big", big)])?;

        let val = spark.createDataFrame(&data)?.cov("small", "big").await?;

        assert_eq!(1.0_f64, val);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_view() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        spark
            .clone()
            .createDataFrame(&data)?
            .createOrReplaceGlobalTempView("people")
            .await?;

        let rows = spark
            .sql("SELECT * FROM global_temp.people")
            .await?
            .collect()
            .await?;

        assert_eq!(rows, data);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_crosstab() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 5, 1, Some(1))
            .select(vec![col("id").alias("col1"), col("id").alias("col2")])
            .crosstab("col1", "col2");

        let res = df.clone().collect().await?;

        assert!(df.columns().await?.contains(&"col1_col2".to_string()));
        assert_eq!(6, res.num_columns());
        Ok(())
    }

    #[tokio::test]
    async fn test_df_crossjoin() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
        let height: ArrayRef = Arc::new(Int64Array::from(vec![60, 55, 63]));

        let data = RecordBatch::try_from_iter(vec![("name", name.clone()), ("age", age)])?;
        let data2 = RecordBatch::try_from_iter(vec![("name", name), ("height", height)])?;

        let df = spark.clone().createDataFrame(&data)?;
        let df2 = spark.createDataFrame(&data2)?;

        let rows = df
            .crossJoin(df2.select("height"))
            .select(["age", "name", "height"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec![
            "Tom", "Tom", "Tom", "Alice", "Alice", "Alice", "Bob", "Bob", "Bob",
        ]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 14, 14, 23, 23, 23, 16, 16, 16]));
        let height: ArrayRef = Arc::new(Int64Array::from(vec![60, 55, 63, 60, 55, 63, 60, 55, 63]));

        let data =
            RecordBatch::try_from_iter(vec![("age", age), ("name", name), ("height", height)])?;

        assert_eq!(data, rows);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_describe() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let res = spark
            .createDataFrame(&data)?
            .describe(Some(["age"]))
            .collect()
            .await?;

        let summary: ArrayRef = Arc::new(StringArray::from(vec![
            "count", "mean", "stddev", "min", "max",
        ]));
        let age: ArrayRef = Arc::new(StringArray::from(vec![
            "3",
            "17.666666666666668",
            "4.725815626252608",
            "14",
            "23",
        ]));

        let schema = Schema::new(vec![
            Field::new("summary", DataType::Utf8, true),
            Field::new("age", DataType::Utf8, true),
        ]);

        let expected = RecordBatch::try_new(Arc::new(schema), vec![summary, age])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_distinct() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let val = spark.createDataFrame(&data)?.distinct().count().await?;

        assert_eq!(3_i64, val);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_drop() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let cols = df.clone().drop("age").columns().await?;

        assert_eq!(vec![String::from("name")], cols);

        let cols = df
            .clone()
            .withColumn("val", lit(1))
            .drop([col("age"), col("name")])
            .columns()
            .await?;

        assert_eq!(vec![String::from("val")], cols);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_drop_duplicates() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Alice", "Alice"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![5, 5, 10]));
        let height: ArrayRef = Arc::new(Int64Array::from(vec![80, 80, 80]));

        let data =
            RecordBatch::try_from_iter(vec![("name", name), ("age", age), ("height", height)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df.clone().drop_duplicates(None).count().await?;

        assert_eq!(res, 2);

        let res = df
            .drop_duplicates(Some(vec!["name", "height"]))
            .count()
            .await?;

        assert_eq!(res, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_dropna() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Tom"),
            None,
        ]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), Some(5), None, None]));
        let height: ArrayRef = Arc::new(Int64Array::from(vec![Some(80), None, None, None]));

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("height", DataType::Int64, true),
        ]);

        let data = RecordBatch::try_new(Arc::new(schema), vec![name, age, height])?;

        let df = spark.createDataFrame(&data)?;

        let res = df.clone().dropna("any", None, None).count().await?;

        assert_eq!(res, 1);

        let res = df.clone().dropna("all", None, None).count().await?;

        assert_eq!(res, 3);

        let res = df
            .clone()
            .dropna("any", None, Some(vec!["name"]))
            .count()
            .await?;

        assert_eq!(res, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_dtypes() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let res = df.dtypes().await?;

        assert!(&res.iter().any(|(col, _kind)| col == "name"));
        Ok(())
    }

    #[tokio::test]
    async fn test_df_except_all() -> Result<(), SparkError> {
        let spark = setup().await;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 10, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 8]));

        let data = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![1, 8]));

        let data2 = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        let df1 = spark.clone().createDataFrame(&data)?;

        let df2 = spark.createDataFrame(&data2)?;

        let output = df1.exceptAll(df2).collect().await?;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 10]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));

        let expected = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        assert_eq!(output, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_explain() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let val = df.explain(None).await?;

        assert!(val.contains("== Physical Plan =="));
        Ok(())
    }

    #[tokio::test]
    async fn test_df_filter() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let output = df.clone().filter("age > 20").count().await?;

        assert_eq!(output, 1);

        let output = df.filter("age == 16").count().await?;

        assert_eq!(output, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_first() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let val = df.first().await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14]));

        let expected = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        assert_eq!(val, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_group_by() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        // AVG
        let val = df
            .clone()
            .groupBy::<Column>(None)
            .avg("age")
            .collect()
            .await?;

        let age: ArrayRef = Arc::new(Float64Array::from(vec![17.666666666666668]));

        let schema = Schema::new(vec![Field::new("avg(age)", DataType::Float64, true)]);
        let expected = RecordBatch::try_new(Arc::new(schema), vec![age])?;

        assert_eq!(val, expected);

        // MAX
        let val = df
            .clone()
            .groupBy::<Column>(None)
            .max("age")
            .collect()
            .await?;

        let age: ArrayRef = Arc::new(Int64Array::from(vec![23]));

        let schema = Schema::new(vec![Field::new("max(age)", DataType::Int64, true)]);
        let expected = RecordBatch::try_new(Arc::new(schema), vec![age])?;

        assert_eq!(val, expected);

        // SUM
        let val = df
            .clone()
            .groupBy::<Column>(None)
            .sum("age")
            .collect()
            .await?;

        let age: ArrayRef = Arc::new(Int64Array::from(vec![53]));

        let schema = Schema::new(vec![Field::new("sum(age)", DataType::Int64, true)]);
        let expected = RecordBatch::try_new(Arc::new(schema), vec![age])?;

        assert_eq!(val, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_head() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let val = df.head(None).await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14]));

        let expected = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        assert_eq!(val, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_hint() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.clone().createDataFrame(&data)?.alias("df1");
        let df2 = spark.createDataFrame(&data)?.alias("df2");

        let df = df.join(
            df2.hint::<Column>("broadcast", None),
            Some(col("df1.name").eq("df2.name")),
            JoinType::Inner,
        );

        let plan = df.explain(Some(ExplainMode::Extended)).await?;

        assert!(plan.contains("UnresolvedHint broadcast"));
        Ok(())
    }

    #[tokio::test]
    async fn test_df_input_files() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/examples/src/main/resources/people.csv"];

        let df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path)?;

        let res = df.inputFiles().await?;

        assert_eq!(res.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_intersect() -> Result<(), SparkError> {
        let spark = setup().await;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 10, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 8]));

        let data = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![1, 8]));

        let data2 = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        let df1 = spark.clone().createDataFrame(&data)?;

        let df2 = spark.createDataFrame(&data2)?;

        let output = df1.intersect(df2).collect().await?;

        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 19]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![1, 8]));

        let expected = RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2)])?;

        assert_eq!(output, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_is_empty() -> Result<(), SparkError> {
        let spark = setup().await;

        let records: ArrayRef = Arc::new(Int64Array::from(vec![] as Vec<i64>));

        let schema = Schema::new(vec![Field::new("record", DataType::Int64, true)]);
        let data = RecordBatch::try_new(Arc::new(schema), vec![records])?;

        let df = spark.clone().createDataFrame(&data)?;

        assert!(df.isEmpty().await?);

        let records: ArrayRef = Arc::new(Int64Array::from(vec![None]));

        let schema = Schema::new(vec![Field::new("record", DataType::Int64, true)]);
        let data = RecordBatch::try_new(Arc::new(schema), vec![records])?;

        let df = spark.clone().createDataFrame(&data)?;

        assert!(!df.isEmpty().await?);

        let records: ArrayRef = Arc::new(Int64Array::from(vec![1]));

        let schema = Schema::new(vec![Field::new("record", DataType::Int64, true)]);
        let data = RecordBatch::try_new(Arc::new(schema), vec![records])?;

        let df = spark.createDataFrame(&data)?;

        assert!(!df.isEmpty().await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_join() -> Result<(), SparkError> {
        let spark = setup().await;

        let data1 = mock_data();

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Bob"]));
        let height: ArrayRef = Arc::new(Int64Array::from(vec![80, 85]));

        let data2 = RecordBatch::try_from_iter(vec![("name", name), ("height", height)])?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![23, 16]));

        let data3 = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        let name: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Tom"),
            None,
        ]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![Some(18), Some(16), None, None]));
        let height: ArrayRef = Arc::new(Int64Array::from(vec![Some(80), None, None, None]));

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("height", DataType::Int64, true),
        ]);

        let data4 = RecordBatch::try_new(Arc::new(schema), vec![name, age, height])?;

        let df1 = spark.clone().createDataFrame(&data1)?.alias("df1");
        let df2 = spark.clone().createDataFrame(&data2)?.alias("df2");
        let df3 = spark.clone().createDataFrame(&data3)?.alias("df3");
        let df4 = spark.createDataFrame(&data4)?.alias("df4");

        // inner join
        let condition = Some(col("df1.name").eq(col("df2.name")));
        let res = df1
            .clone()
            .join(df2.clone(), condition, JoinType::Inner)
            .select(["df1.name", "df2.height"])
            .collect()
            .await?;

        assert_eq!(2, res.num_rows());

        // complex join
        // results in one record for Bob
        let name = col("df1.name").eq(col("df4.name"));
        let age = col("df1.age").eq(col("df4.age"));
        let condition = Some(name.and(age));

        let res = df1
            .clone()
            .join(df4.clone(), condition, JoinType::Inner)
            .collect()
            .await?;

        assert_eq!(1, res.num_rows());

        // left outer join
        // two records "Bob" & "Jorge"
        let condition = Some(col("df1.name").eq(col("df2.name")));
        let res = df1
            .clone()
            .join(df2.clone(), condition, JoinType::FullOuter)
            .select(["df1.name", "df2.height"])
            .collect()
            .await?;

        assert_eq!(3, res.num_rows());

        let name = col("df1.name").eq(col("df3.name"));
        let age = col("df1.age").eq(col("df3.age"));
        let condition = Some(name.and(age));

        let res = df1
            .clone()
            .join(df3.clone(), condition, JoinType::FullOuter)
            .select(["df1.name", "df3.age"])
            .collect()
            .await?;

        assert_eq!(3, res.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn test_df_limit() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let val = df.clone().limit(1).collect().await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14]));

        let expected = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        assert_eq!(val, expected);

        let val = df.clone().limit(0).collect().await?;

        assert_eq!(0, val.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn test_df_select() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        // select *
        let val = df.clone().select("*").collect().await?;

        assert_eq!(2, val.num_columns());

        // single select
        let val = df.clone().select("name").collect().await?;

        assert_eq!(1, val.num_columns());

        // select slice of &str
        let val = df.clone().select(["name", "age"]).collect().await?;

        assert_eq!(2, val.num_columns());

        // select vec of &str
        let val = df.clone().select(vec!["name", "age"]).collect().await?;

        assert_eq!(2, val.num_columns());

        // select slice of columns
        let val = df
            .clone()
            .select([col("name"), col("age")])
            .collect()
            .await?;

        assert_eq!(2, val.num_columns());
        Ok(())
    }

    #[tokio::test]
    async fn test_df_select_expr() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let val = df.selectExpr(["age * 2", "abs(age)"]).collect().await?;

        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
        let age2: ArrayRef = Arc::new(Int64Array::from(vec![28, 46, 32]));

        let expected = RecordBatch::try_from_iter(vec![("(age * 2)", age2), ("abs(age)", age)])?;

        assert_eq!(val, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_df_with_columns() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let cols = [("age2", col("age") + lit(2)), ("age3", col("age") + lit(3))];

        let val = df
            .clone()
            .withColumns(cols)
            .select(["name", "age", "age2", "age3"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
        let age2: ArrayRef = Arc::new(Int64Array::from(vec![16, 25, 18]));
        let age3: ArrayRef = Arc::new(Int64Array::from(vec![17, 26, 19]));

        let expected = RecordBatch::try_from_iter(vec![
            ("name", name),
            ("age", age),
            ("age2", age2),
            ("age3", age3),
        ])?;

        assert_eq!(&val, &expected);

        // As a hashmap
        let cols = HashMap::from([("age2", col("age") + lit(2)), ("age3", col("age") + lit(3))]);
        let val = df
            .clone()
            .withColumns(cols)
            .select(["name", "age", "age2", "age3"])
            .collect()
            .await?;

        assert_eq!(&val, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_sort() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 100, 1, Some(1))
            .sort(vec![col("id").desc()]);

        let rows = df.limit(1).collect().await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![99]));

        let expected_batch = RecordBatch::try_from_iter(vec![("id", a)])?;

        assert_eq!(expected_batch, rows);
        Ok(())
    }

    #[tokio::test]
    async fn test_df_unpivot() -> Result<(), SparkError> {
        let spark = setup().await;

        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![11, 12]));
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![1.1, 1.2]));

        let data = RecordBatch::try_from_iter(vec![("id", ids), ("int", ints), ("float", floats)])?;

        let df = spark.createDataFrame(&data)?;

        let df = df.unpivot("id", Some(["int", "float"]), "var", "val");

        let res = df.collect().await?;

        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 2]));
        let var: ArrayRef = Arc::new(StringArray::from(vec!["int", "float", "int", "float"]));
        let val: ArrayRef = Arc::new(Float32Array::from(vec![11.0, 1.1, 12.0, 1.2]));

        let expected = RecordBatch::try_from_iter(vec![("id", ids), ("var", var), ("val", val)])?;

        assert_eq!(expected, res);

        Ok(())
    }
}
