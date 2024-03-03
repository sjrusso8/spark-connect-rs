//! DataFrame with Reader/Writer repesentation

use std::collections::HashMap;

use crate::column::Column;
use crate::expressions::{ToFilterExpr, ToVecExpr};
use crate::plan::LogicalPlanBuilder;
pub use crate::readwriter::{DataFrameReader, DataFrameWriter};
use crate::session::SparkSession;
use crate::spark;

use spark::relation::RelType;

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

    pub fn contains(&mut self, condition: Column) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.contains(condition),
        )
    }

    pub fn sort(&mut self, cols: Vec<Column>) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.sort(cols))
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

    /// Returns a new [DataFrame] by renaming multiple columns from a
    /// `HashMap<String, String>` containing the `existing` as the key
    /// and the `new` as the value.
    ///
    #[allow(non_snake_case)]
    pub fn withColumnsRenamed(&mut self, cols: HashMap<String, String>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.with_columns_renamed(cols),
        )
    }

    /// Returns a new [DataFrame] without the specified columns
    pub fn drop(&mut self, cols: Vec<String>) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.drop(cols))
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

    /// Returns a new [DataFrame] partitioned by the given partition number and shuffle
    /// option
    ///
    /// # Arguments
    ///
    /// * `num_partitions`: the target number of partitions
    /// * (optional) `shuffle`: to induce a shuffle. Default is `false`
    ///
    pub fn repartition(&mut self, num_partitions: i32, shuffle: Option<bool>) -> DataFrame {
        DataFrame::new(
            self.spark_session.clone(),
            self.logical_plan.repartition(num_partitions, shuffle),
        )
    }

    /// Returns a new [DataFrame] by skiping the first n rows
    pub fn offset(&mut self, num: i32) -> DataFrame {
        DataFrame::new(self.spark_session.clone(), self.logical_plan.offset(num))
    }

    /// Returns the schema of this DataFrame as a [spark::analyze_plan_response::Schema]
    /// which contains the schema of a DataFrame
    pub async fn schema(&mut self) -> spark::analyze_plan_response::Schema {
        let analyze = Some(spark::analyze_plan_request::Analyze::Schema(
            spark::analyze_plan_request::Schema {
                plan: Some(self.logical_plan.clone().build_plan_root()),
            },
        ));

        let schema = self.spark_session.analyze_plan(analyze).await;

        match schema {
            Some(spark::analyze_plan_response::Result::Schema(schema)) => schema,
            _ => panic!("Unexpected result"),
        }
    }

    /// Prints the [spark::Plan] to the console
    ///
    /// # Arguments:
    /// * `mode`: &str. Defaults to `unspecified`
    ///     - `simple`
    ///     - `extended`
    ///     - `codegen`
    ///     - `cost`
    ///     - `formatted`
    ///     - `unspecified`
    ///
    pub async fn explain(&mut self, mode: &str) {
        let explain_mode = match mode {
            "simple" => spark::analyze_plan_request::explain::ExplainMode::Simple,
            "extended" => spark::analyze_plan_request::explain::ExplainMode::Extended,
            "codegen" => spark::analyze_plan_request::explain::ExplainMode::Codegen,
            "cost" => spark::analyze_plan_request::explain::ExplainMode::Cost,
            "formatted" => spark::analyze_plan_request::explain::ExplainMode::Formatted,
            _ => spark::analyze_plan_request::explain::ExplainMode::Unspecified,
        };

        let analyze = Some(spark::analyze_plan_request::Analyze::Explain(
            spark::analyze_plan_request::Explain {
                plan: Some(self.logical_plan.clone().build_plan_root()),
                explain_mode: explain_mode.into(),
            },
        ));

        let explain = match self.spark_session.analyze_plan(analyze).await {
            Some(spark::analyze_plan_response::Result::Explain(explain)) => explain,
            _ => todo!("Not implemented"),
        };

        println!("{}", explain.explain_string)
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

        let plan = LogicalPlanBuilder::build_plan_cmd(command_type);

        self.spark_session.consume_plan(Some(plan)).await.unwrap();

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
            input: self.logical_plan.clone().relation_input(),
            num_rows: num_rows.unwrap_or(10),
            truncate: truncate.unwrap_or(0),
            vertical: vertical.unwrap_or(false),
        }));

        let plan = LogicalPlanBuilder::from(show_expr).build_plan_root();

        let rows = self.spark_session.consume_plan(Some(plan)).await?;

        let _ = pretty::print_batches(rows.as_slice());
        Ok(())
    }

    /// Returns the last `n` rows as vector of [RecordBatch]
    ///
    /// Running tail requires moving the data and results in an action
    ///
    pub async fn tail(&mut self, limit: i32) -> Result<Vec<RecordBatch>, ArrowError> {
        let limit_expr = RelType::Tail(Box::new(spark::Tail {
            input: self.logical_plan.clone().relation_input(),
            limit,
        }));

        let plan = LogicalPlanBuilder::from(limit_expr).build_plan_root();

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
