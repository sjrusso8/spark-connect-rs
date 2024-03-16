//! DataFrame with Reader/Writer repesentation

use std::collections::HashMap;

use crate::column::Column;
use crate::errors::SparkError;
use crate::expressions::{ToExpr, ToFilterExpr, ToVecExpr};
use crate::plan::LogicalPlanBuilder;
pub use crate::readwriter::{DataFrameReader, DataFrameWriter};
use crate::session::SparkSession;
use crate::spark;
use crate::storage;
pub use spark::analyze_plan_request::explain::ExplainMode;
pub use spark::join::JoinType;
pub use spark::write_operation::SaveMode;

use spark::relation::RelType;

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

/// DataFrame is composed of a `spark_session` connecot ting to a remote
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

    fn check_same_session(&self, other: &DataFrame) -> Result<(), SparkError> {
        if self.spark_session.session_id != other.spark_session.session_id {
            return Err(SparkError::AnalysisException(
                "Spark Session is not the same!".to_string(),
            ));
        };

        Ok(())
    }

    /// Returns a new [DataFrame] with an alias set.
    pub fn alias(&mut self, alias: &str) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.alias(alias))
    }

    /// Persists the [DataFrame] with the default [storage::StorageLevel::MemoryAndDiskDeser] (MEMORY_AND_DISK_DESER).
    pub async fn cache(&mut self) -> DataFrame {
        self.persist(storage::StorageLevel::MemoryAndDiskDeser)
            .await
    }

    /// Returns a new [DataFrame] that has exactly `num_partitions` partitions.
    pub fn coalesce(&mut self, num_partitions: u32) -> DataFrame {
        self.repartition(num_partitions, Some(false))
    }

    /// Selects column based on the column name specified as a regex and returns it as [Column].
    #[allow(non_snake_case)]
    pub fn colRegex(self, col_name: &str) -> Column {
        let expr = spark::Expression {
            expr_type: Some(spark::expression::ExprType::UnresolvedRegex(
                spark::expression::UnresolvedRegex {
                    col_name: col_name.to_string(),
                    plan_id: Some(self.logical_plan.plan_id),
                },
            )),
        };
        Column::from(expr)
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
    pub async fn collect(&mut self) -> Result<Vec<RecordBatch>, SparkError> {
        let rows = self
            .spark_session
            .consume_plan(Some(self.logical_plan.clone().build_plan_root()))
            .await
            .unwrap();

        Ok(rows)
    }

    /// Retrieves the names of all columns in the [DataFrame] as a `Vec<String>`.
    /// The order of the column names in the list reflects their order in the [DataFrame].
    pub async fn columns(&mut self) -> Vec<String> {
        let schema = self.schema().await.unwrap();

        let struct_val = schema.kind.unwrap();

        match struct_val {
            spark::data_type::Kind::Struct(val) => val
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect(),
            _ => unimplemented!("Unexpected schema response"),
        }
    }
    /// Calculates the correlation of two columns of a [DataFrame] as a `f64`.
    /// Currently only supports the Pearson Correlation Coefficient.
    pub async fn corr(&mut self, col1: &str, col2: &str) -> Option<f64> {
        let result = DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.corr(col1, col2),
        )
        .collect()
        .await
        .unwrap();

        let col = result[0].column(0);

        let data: Option<&PrimitiveArray<Float64Type>> = match col.data_type() {
            DataType::Float64 => col.as_any().downcast_ref(),
            _ => panic!("Expected Float64 in response type"),
        };

        Some(data?.value(0))
    }

    /// Calculate the sample covariance for the given columns, specified by their names, as a f64
    pub async fn cov(&mut self, col1: &str, col2: &str) -> Option<f64> {
        let result = DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.cov(col1, col2),
        )
        .collect()
        .await
        .unwrap();

        let col = result[0].column(0);

        let data: Option<&PrimitiveArray<Float64Type>> = match col.data_type() {
            DataType::Float64 => col.as_any().downcast_ref(),
            _ => panic!("Expected Float64 in response type"),
        };

        Some(data?.value(0))
    }

    #[allow(non_snake_case, dead_code)]
    pub async fn createTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), false, false)
            .await
            .unwrap()
    }

    #[allow(non_snake_case, dead_code)]
    pub async fn createGlobalTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), true, false)
            .await
            .unwrap()
    }

    #[allow(non_snake_case, dead_code)]
    pub async fn createOrReplaceGlobalTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), true, true)
            .await
            .unwrap()
    }

    #[allow(non_snake_case, dead_code)]
    pub async fn createOrReplaceTempView(&mut self, name: &str) {
        self.create_view_cmd(name.to_string(), false, true)
            .await
            .unwrap()
    }

    async fn create_view_cmd(
        &mut self,
        name: String,
        is_global: bool,
        replace: bool,
    ) -> Result<(), SparkError> {
        let command_type =
            spark::command::CommandType::CreateDataframeView(spark::CreateDataFrameViewCommand {
                input: Some(self.logical_plan.clone().relation),
                name,
                is_global,
                replace,
            });

        let plan = LogicalPlanBuilder::build_plan_cmd(command_type);

        self.spark_session.consume_plan(Some(plan)).await.unwrap();

        Ok(())
    }

    /// Returns the cartesian product with another [DataFrame].
    #[allow(non_snake_case)]
    pub fn crossJoin(&mut self, other: DataFrame) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan
                .join(other.logical_plan, None::<&str>, JoinType::Cross, vec![]),
        )
    }

    /// Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
    pub fn crosstab(&mut self, col1: &str, col2: &str) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.crosstab(col1, col2),
        )
    }

    // Computes basic statistics for numeric and string columns. This includes count, mean, stddev, min, and max.
    // If no columns are given, this function computes statistics for all numerical or string columns.
    pub fn describe(&mut self, cols: Option<Vec<&str>>) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.describe(cols))
    }

    /// Returns a new [DataFrame] containing the distinct rows in this [DataFrame].
    pub fn distinct(&mut self) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.distinct())
    }

    /// Returns a new [DataFrame] without the specified columns
    pub fn drop<T: ToVecExpr>(&mut self, cols: T) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.drop(cols))
    }

    /// Return a new [DataFrame] with duplicate rows removed,
    /// optionally only considering certain columns from a `Vec<String>`
    ///
    /// If no columns are supplied then it all columns are used
    ///
    /// Alias for `dropDuplciates`
    ///
    pub fn drop_duplicates(&mut self, cols: Option<Vec<&str>>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.drop_duplicates(cols),
        )
    }

    #[allow(non_snake_case)]
    pub fn dropDuplicates(&mut self, cols: Option<Vec<&str>>) -> DataFrame {
        self.drop_duplicates(cols)
    }

    pub fn dropna(
        &mut self,
        how: &str,
        threshold: Option<i32>,
        subset: Option<Vec<&str>>,
    ) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.dropna(how, threshold, subset),
        )
    }

    /// Returns all column names and their data types as a Vec containing
    /// the field name as a String and the [spark::data_type::Kind] enum
    pub async fn dtypes(&mut self) -> Vec<(String, Option<spark::data_type::Kind>)> {
        let schema = self.schema().await.unwrap();

        let struct_val = schema.kind.unwrap();

        match struct_val {
            spark::data_type::Kind::Struct(val) => val
                .fields
                .iter()
                .map(|field| {
                    (
                        field.name.to_string(),
                        field.data_type.clone().unwrap().kind,
                    )
                })
                .collect(),
            _ => unimplemented!("Unexpected schema response"),
        }
    }

    #[allow(non_snake_case)]
    pub fn exceptAll(&mut self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
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
    pub async fn explain(&mut self, mode: Option<ExplainMode>) -> Result<String, SparkError> {
        let explain_mode = match mode {
            Some(mode) => mode,
            None => ExplainMode::Simple,
        };

        let analyze = Some(spark::analyze_plan_request::Analyze::Explain(
            spark::analyze_plan_request::Explain {
                plan: Some(self.logical_plan.clone().build_plan_root()),
                explain_mode: explain_mode.into(),
            },
        ));

        let explain = match self.spark_session.analyze_plan(analyze).await {
            Some(spark::analyze_plan_response::Result::Explain(explain)) => explain,
            _ => panic!("Unexpected response from Analyze Plan"),
        };

        println!("{}", explain.explain_string);
        Ok(explain.explain_string)
    }

    /// Filters rows using a given conditions and returns a new [DataFrame]
    ///
    /// # Example:
    /// ```rust
    /// async {
    ///     df.filter("salary > 4000").collect().await?;
    /// }
    /// ```
    pub fn filter<T: ToFilterExpr>(&mut self, condition: T) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.filter(condition),
        )
    }

    pub async fn first(&mut self) -> Vec<RecordBatch> {
        self.head(None).await
    }

    #[allow(non_snake_case)]
    pub fn freqItems(&mut self, cols: Vec<&str>, support: Option<f64>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.freqItems(cols, support),
        )
    }

    pub async fn head(&mut self, n: Option<i32>) -> Vec<RecordBatch> {
        self.limit(n.unwrap_or(1)).collect().await.unwrap()
    }

    pub fn hint<T: ToVecExpr>(&mut self, name: &str, parameters: Option<T>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.hint(name, parameters),
        )
    }

    #[allow(non_snake_case)]
    pub async fn inputFiles(&mut self) -> Option<Vec<String>> {
        let input_files = Some(spark::analyze_plan_request::Analyze::InputFiles(
            spark::analyze_plan_request::InputFiles {
                plan: Some(self.logical_plan.clone().build_plan_root()),
            },
        ));

        let resp = self.spark_session.analyze_plan(input_files).await;

        match resp {
            Some(spark::analyze_plan_response::Result::InputFiles(files)) => Some(files.files),
            _ => panic!("Unexpected response from Analyze plan"),
        }
    }

    pub fn intersect(&mut self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.intersect(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub fn intersectAll(&mut self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.intersectAll(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub async fn isEmpty(&mut self) -> bool {
        let val = &self.select("*").limit(1).collect().await.unwrap()[0];

        val.num_rows() == 0
    }

    /// Joins with another DataFrame, using the given join expression.
    ///
    /// # Example:
    /// ```rust
    ///
    /// use spark_connect_rs::functionas::*
    /// use spark_connect_rs::dataframe::JoinType;
    ///
    /// async {
    ///     // join two dataframes where `id` == `name`
    ///     let condition = Some(col("id").eq("name"));
    ///     let df = df.join(df2, condition, JoinType::Inner);
    /// }
    /// ```

    pub fn join<T: ToExpr>(&mut self, other: DataFrame, on: Option<T>, how: JoinType) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
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
    pub fn limit(&mut self, limit: i32) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.limit(limit))
    }

    /// Returns a new [DataFrame] by skiping the first n rows
    pub fn offset(&mut self, num: i32) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.offset(num))
    }

    #[allow(non_snake_case)]
    pub fn orderBy(&mut self, cols: Vec<Column>) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.sort(cols))
    }

    pub async fn persist(&mut self, storage_level: storage::StorageLevel) -> DataFrame {
        let analyze = Some(spark::analyze_plan_request::Analyze::Persist(
            spark::analyze_plan_request::Persist {
                relation: Some(self.logical_plan.clone().relation),
                storage_level: Some(storage_level.into()),
            },
        ));

        self.spark_session.analyze_plan(analyze).await;

        DataFrame::new(self.spark_session.clone(), self.logical_plan.clone())
    }

    #[allow(non_snake_case)]
    pub async fn printSchema(&mut self, level: Option<i32>) -> Option<String> {
        let input_files = Some(spark::analyze_plan_request::Analyze::TreeString(
            spark::analyze_plan_request::TreeString {
                plan: Some(self.logical_plan.clone().build_plan_root()),
                level,
            },
        ));

        let resp = self.spark_session.analyze_plan(input_files).await;

        match resp {
            Some(spark::analyze_plan_response::Result::TreeString(tstring)) => {
                Some(tstring.tree_string)
            }
            _ => panic!("Unexpected response from Analyze plan"),
        }
    }

    /// Returns a new [DataFrame] partitioned by the given partition number and shuffle option
    ///
    /// # Arguments
    ///
    /// * `num_partitions`: the target number of partitions
    /// * (optional) `shuffle`: to induce a shuffle. Default is `false`
    ///
    pub fn repartition(&mut self, num_partitions: u32, shuffle: Option<bool>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.repartition(num_partitions, shuffle),
        )
    }

    #[allow(non_snake_case)]
    pub async fn sameSemantics(&mut self, other: DataFrame) -> bool {
        let same_semantics = Some(spark::analyze_plan_request::Analyze::SameSemantics(
            spark::analyze_plan_request::SameSemantics {
                target_plan: Some(self.logical_plan.clone().build_plan_root()),
                other_plan: Some(other.logical_plan.clone().build_plan_root()),
            },
        ));

        let resp = self.spark_session.analyze_plan(same_semantics).await;

        match resp {
            Some(spark::analyze_plan_response::Result::SameSemantics(semantics)) => {
                semantics.result
            }
            _ => panic!("Unexpected response from Analyze plan"),
        }
    }

    /// Returns a sampled subset of this [DataFrame]
    pub fn sample(
        &mut self,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: Option<bool>,
        seed: Option<i64>,
    ) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan
                .sample(lower_bound, upper_bound, with_replacement, seed),
        )
    }

    /// Returns the schema of this DataFrame as a [spark::DataType]
    /// which contains the schema of a DataFrame
    pub async fn schema(&mut self) -> Option<spark::DataType> {
        let analyze = Some(spark::analyze_plan_request::Analyze::Schema(
            spark::analyze_plan_request::Schema {
                plan: Some(self.logical_plan.clone().build_plan_root()),
            },
        ));

        let schema = self.spark_session.analyze_plan(analyze).await;

        match schema {
            Some(spark::analyze_plan_response::Result::Schema(schema)) => schema.schema,
            _ => panic!("Unexpected result"),
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
    ///     df.select(vec![col("age"), col("name")]).collect().await?;
    /// }
    /// ```
    pub fn select<T: ToVecExpr>(&mut self, cols: T) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.select(cols))
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
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.select_expr(cols),
        )
    }

    #[allow(non_snake_case)]
    pub async fn semanticHash(&mut self) -> i32 {
        let semantic_hash = Some(spark::analyze_plan_request::Analyze::SemanticHash(
            spark::analyze_plan_request::SemanticHash {
                plan: Some(self.logical_plan.clone().build_plan_root()),
            },
        ));

        let resp = self.spark_session.analyze_plan(semantic_hash).await;

        match resp {
            Some(spark::analyze_plan_response::Result::SemanticHash(hash)) => hash.result,
            _ => panic!("Unexpected response from Analyze plan"),
        }
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
    ) -> Result<(), SparkError> {
        let show_expr = RelType::ShowString(Box::new(spark::ShowString {
            input: self.logical_plan.clone().relation_input(),
            num_rows: num_rows.unwrap_or(10),
            truncate: truncate.unwrap_or(0),
            vertical: vertical.unwrap_or(false),
        }));

        let plan = LogicalPlanBuilder::from(show_expr).build_plan_root();

        let rows = self.spark_session.consume_plan(Some(plan)).await?;

        pretty::print_batches(rows.as_slice())?;
        Ok(())
    }

    pub fn sort(&mut self, cols: Vec<Column>) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.sort(cols))
    }

    #[allow(non_snake_case)]
    pub fn sparkSession(self) -> SparkSession {
        self.spark_session
    }

    #[allow(non_snake_case)]
    pub async fn storageLevel(&mut self) -> Option<storage::StorageLevel> {
        let storage_level = Some(spark::analyze_plan_request::Analyze::GetStorageLevel(
            spark::analyze_plan_request::GetStorageLevel {
                relation: Some(self.logical_plan.clone().relation),
            },
        ));

        let resp = self.spark_session.analyze_plan(storage_level).await;

        match resp {
            Some(spark::analyze_plan_response::Result::GetStorageLevel(level)) => {
                Some(level.storage_level.unwrap().into())
            }
            _ => panic!("Unexpected response from Analyze plan"),
        }
    }

    pub fn subtract(&mut self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.substract(other.logical_plan),
        )
    }

    /// Returns the last `n` rows as vector of [RecordBatch]
    ///
    /// Running tail requires moving the data and results in an action
    ///
    pub async fn tail(&mut self, limit: i32) -> Result<Vec<RecordBatch>, SparkError> {
        let limit_expr = RelType::Tail(Box::new(spark::Tail {
            input: self.logical_plan.clone().relation_input(),
            limit,
        }));

        let plan = LogicalPlanBuilder::from(limit_expr).build_plan_root();

        let rows = self.spark_session.consume_plan(Some(plan)).await.unwrap();

        Ok(rows)
    }

    pub async fn take(&mut self, n: i32) -> Vec<RecordBatch> {
        self.limit(n).collect().await.unwrap()
    }

    #[allow(non_snake_case)]
    pub fn toDF(&mut self, cols: Vec<&str>) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.to_df(cols))
    }

    pub fn union(&mut self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.unionAll(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub fn unionAll(&mut self, other: DataFrame) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.unionAll(other.logical_plan),
        )
    }

    #[allow(non_snake_case)]
    pub fn unionByName(
        &mut self,
        other: DataFrame,
        allow_missing_columns: Option<bool>,
    ) -> DataFrame {
        self.check_same_session(&other).unwrap();
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan
                .unionByName(other.logical_plan, allow_missing_columns),
        )
    }

    pub async fn unpersist(self, blocking: Option<bool>) -> DataFrame {
        let unpersist = Some(spark::analyze_plan_request::Analyze::Unpersist(
            spark::analyze_plan_request::Unpersist {
                relation: Some(self.logical_plan.clone().relation),
                blocking,
            },
        ));

        let resp = self.spark_session.clone().analyze_plan(unpersist).await;

        match resp {
            Some(spark::analyze_plan_response::Result::Unpersist(_)) => self,
            _ => panic!("Unexpected response from Analyze plan"),
        }
    }

    #[allow(non_snake_case)]
    pub fn withColumn(&mut self, colName: &str, col: Column) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.withColumn(colName, col),
        )
    }

    #[allow(non_snake_case)]
    pub fn withColumns(&mut self, colMap: HashMap<&str, Column>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.withColumns(colMap),
        )
    }

    /// Returns a new [DataFrame] by renaming multiple columns from a
    /// `HashMap<String, String>` containing the `existing` as the key
    /// and the `new` as the value.
    #[allow(non_snake_case)]
    pub fn withColumnsRenamed(&mut self, cols: HashMap<String, String>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.withColumnsRenamed(cols),
        )
    }
    /// Returns a [DataFrameWriter] struct based on the current [DataFrame]
    pub fn write(self) -> DataFrameWriter {
        DataFrameWriter::new(self)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::functions::*;
    use crate::SparkSessionBuilder;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_df".to_string();

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_alias() {
        let spark = setup().await;

        let mut df = spark
            .clone()
            .range(None, 2, 1, Some(1))
            .select(vec![lit("John").alias("name"), lit(1).alias("value")]);

        let mut df_as1 = df.clone().alias("df_as1");
        let df_as2 = df.alias("df_as2");

        let condition = Some(col("df_as1.name").eq(col("df_as2.name")));

        let mut joined_df = df_as1.join(df_as2, condition, JoinType::Inner);

        let res = &joined_df
            .select(vec!["df_as1.name", "df_as2.name", "df_as2.value"])
            .collect()
            .await
            .unwrap()[0];

        assert_eq!(res.num_columns(), 3);
        assert_eq!(res.num_rows(), 4);
        assert_eq!(
            vec![
                "name".to_string(),
                "value".to_string(),
                "name".to_string(),
                "value".to_string()
            ],
            joined_df.columns().await
        )
    }

    #[tokio::test]
    async fn test_cache() {
        let spark = setup().await;

        let mut df = spark.range(None, 2, 1, None);
        df.cache().await;

        let exp = df.explain(None).await.unwrap();
        assert!(exp.contains("InMemoryTableScan"));
    }

    #[tokio::test]
    async fn test_colregex() {
        let spark = setup().await;

        let df = spark
            .range(None, 1, 1, Some(1))
            .select(vec![lit(1).alias("Col1"), lit(1).alias("Col2")]);

        let res = &df
            .clone()
            .select(df.colRegex("`(Col1)?+.+`"))
            .collect()
            .await
            .unwrap()[0];

        assert_eq!(
            vec![&"Col2".to_string(),],
            res.schema()
                .fields()
                .iter()
                .map(|val| val.name())
                .collect::<Vec<_>>()
        )
    }

    #[tokio::test]
    async fn test_coalesce() {
        let spark = setup().await;

        // partition num of 5 would create 5 different values for
        // spark_partition_id
        let val = &spark
            .range(None, 10, 1, Some(5))
            .coalesce(1)
            .select(spark_partition_id().alias("partition"))
            .distinct()
            .collect()
            .await
            .unwrap()[0];

        assert_eq!(1, val.num_rows())
    }

    #[tokio::test]
    async fn test_columns() {
        let spark = setup().await;

        let cols = spark
            .clone()
            .range(None, 1, 1, Some(1))
            .select(vec![
                lit(1).alias("Col1"),
                lit(1).alias("Col2"),
                lit(1).alias("Col3"),
            ])
            .columns()
            .await;

        assert_eq!(
            vec!["Col1".to_string(), "Col2".to_string(), "Col3".to_string()],
            cols
        );

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let cols = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths)
            .columns()
            .await;

        let expected = vec![
            String::from("name"),
            String::from("age"),
            String::from("job"),
        ];

        assert_eq!(cols, expected)
    }

    #[tokio::test]
    async fn test_corr() {
        let spark = setup().await;

        let value = spark
            .range(None, 10, 1, Some(1))
            .select(vec![col("id").alias("col1"), col("id").alias("col2")])
            .corr("col1", "col2")
            .await
            .unwrap();

        assert_eq!(1.0, value)
    }

    #[tokio::test]
    async fn test_cov() {
        let spark = setup().await;

        let value = spark
            .range(None, 10, 1, Some(1))
            .select(vec![col("id").alias("col1"), col("id").alias("col2")])
            .cov("col1", "col2")
            .await
            .unwrap();

        assert_eq!(9.0, value.round())
    }

    #[tokio::test]
    async fn test_crosstab() {
        let spark = setup().await;

        let mut df = spark
            .range(None, 5, 1, Some(1))
            .select(vec![col("id").alias("col1"), col("id").alias("col2")])
            .crosstab("col1", "col2");

        let res = &df.collect().await.unwrap()[0];

        assert!(df.columns().await.contains(&"col1_col2".to_string()));
        assert_eq!(6, res.num_columns())
    }

    #[tokio::test]
    async fn test_crossjoin() {
        let spark = setup().await;

        let mut df = spark
            .clone()
            .range(None, 2, 1, Some(1))
            .select(vec![lit("John").alias("name"), col("id").alias("value")])
            .alias("df1");

        let df2 = spark
            .clone()
            .range(Some(4), 6, 1, Some(1))
            .select(vec![lit("Steve").alias("name"), col("id").alias("value")])
            .alias("df2");

        let mut df3 = df
            .crossJoin(df2)
            .select(vec!["df1.name", "df2.name", "df2.value"]);

        let res = &df3.collect().await.unwrap()[0];

        assert_eq!(res.num_columns(), 3);
        assert_eq!(res.num_rows(), 4);
        assert_eq!(
            vec!["name".to_string(), "name".to_string(), "value".to_string()],
            df3.columns().await
        )
    }

    #[tokio::test]
    async fn test_describe() {
        let spark = setup().await;

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths);

        let mut df = df
            .select(col("age").cast("int").alias("age_int"))
            .describe(Some(vec!["age_int"]));

        let res = &df.collect().await.unwrap()[0];

        assert!(df.columns().await.contains(&"summary".to_string()));
        assert_eq!(5, res.num_rows());
    }

    #[tokio::test]
    async fn test_distinct() {
        let spark = setup().await;

        let mut df = spark
            .range(None, 100, 1, Some(1))
            .select(lit(1).alias("val"));

        let mut df_dist = df.distinct();

        let res = &df.collect().await.unwrap()[0];
        let res_dist = &df_dist.collect().await.unwrap()[0];

        assert_eq!(100, res.num_rows());
        assert_eq!(1, res_dist.num_rows())
    }

    #[tokio::test]
    async fn test_drop() {
        let spark = setup().await;

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths);

        let mut df = df.drop(vec!["age", "job"]);

        assert!(!df.columns().await.contains(&"age".to_string()));
        assert!(!df.columns().await.contains(&"job".to_string()));
        assert_eq!(1, df.columns().await.len());
    }

    #[tokio::test]
    async fn test_join() {
        let spark = setup().await;

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let mut df = spark
            .clone()
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths)
            .alias("df");

        let mut df1 = spark
            .clone()
            .range(None, 1, 1, Some(1))
            .select(vec![lit("Bob").alias("name"), lit(1).alias("id")])
            .alias("df1");

        let mut df2 = spark
            .clone()
            .range(None, 1, 1, Some(1))
            .select(vec![lit("Steve").alias("name"), lit(2).alias("id")])
            .alias("df2");

        // inner join
        // only a single record for "Bob"
        let res = &df
            .join(
                df1.clone(),
                Some(col("df.name").eq(col("df1.name"))),
                JoinType::Inner,
            )
            .collect()
            .await
            .unwrap()[0];

        assert_eq!(1, res.num_rows());

        // left outer join
        // two records "Bob" & "Jorge"
        let res = &df
            .join(
                df1,
                Some(col("df.name").eq(col("df1.name"))),
                JoinType::LeftOuter,
            )
            .collect()
            .await
            .unwrap()[0];

        assert_eq!(2, res.num_rows());

        // left anti join
        // one record "Steve"
        let res = &df2
            .join(
                df,
                Some(col("df2.name").eq(col("df.name"))),
                JoinType::LeftAnti,
            )
            .collect()
            .await
            .unwrap()[0];

        assert_eq!(1, res.num_rows());
    }
}
