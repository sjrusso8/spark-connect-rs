// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A DataFrame created with an aggregate statement

use crate::column::Column;
use crate::dataframe::DataFrame;
use crate::plan::LogicalPlanBuilder;

use crate::functions::{invoke_func, lit};

use crate::spark;
use crate::spark::aggregate::GroupType;

/// A set of methods for aggregations on a [DataFrame], created by DataFrame.groupBy().
#[derive(Clone, Debug)]
pub struct GroupedData {
    df: DataFrame,
    group_type: GroupType,
    grouping_cols: Vec<spark::Expression>,
    pivot_col: Option<spark::Expression>,
    pivot_vals: Option<Vec<spark::expression::Literal>>,
}

impl GroupedData {
    pub fn new(
        df: DataFrame,
        group_type: GroupType,
        grouping_cols: Vec<spark::Expression>,
        pivot_col: Option<spark::Expression>,
        pivot_vals: Option<Vec<spark::expression::Literal>>,
    ) -> GroupedData {
        Self {
            df,
            group_type,
            grouping_cols,
            pivot_col,
            pivot_vals,
        }
    }

    /// Compute aggregates and returns the result as a [DataFrame]
    pub fn agg<I, S>(self, exprs: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        let plan = LogicalPlanBuilder::aggregate(
            self.df.plan,
            self.group_type,
            self.grouping_cols,
            exprs,
            self.pivot_col,
            self.pivot_vals,
        );

        DataFrame {
            spark_session: self.df.spark_session,
            plan,
        }
    }

    /// Computes average values for each numeric columns for each group.
    pub fn avg<I, S>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        self.agg([invoke_func("avg", cols)])
    }

    /// Computes the min value for each numeric column for each group.
    pub fn min<I, S>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        self.agg([invoke_func("min", cols)])
    }

    /// Computes the max value for each numeric columns for each group.
    pub fn max<I, S>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        self.agg([invoke_func("max", cols)])
    }

    /// Computes the sum for each numeric columns for each group.
    pub fn sum<I, S>(self, cols: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        self.agg([invoke_func("sum", cols)])
    }

    /// Counts the number of records for each group.
    pub fn count(self) -> DataFrame {
        self.agg([invoke_func("count", [lit(1).alias("count")])])
    }

    /// Pivots a column of the current [DataFrame] and perform the specified aggregation
    pub fn pivot(self, col: &str, values: Option<Vec<&str>>) -> GroupedData {
        let pivot_vals = values.map(|vals| vals.iter().map(|val| val.to_string().into()).collect());

        GroupedData::new(
            self.df,
            GroupType::Pivot,
            self.grouping_cols,
            Some(Column::from(col).into()),
            pivot_vals,
        )
    }
}

#[cfg(test)]
mod tests {

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::errors::SparkError;
    use crate::SparkSession;
    use crate::SparkSessionBuilder;

    use crate::functions::col;

    use crate::column::Column;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection =
            "sc://127.0.0.1:15002/;user_id=rust_group;session_id=02c25694-e875-4a25-9955-bc5bc56c4ade";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_group_count() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 100, 1, Some(8));

        let res = df.group_by::<Vec<Column>>(None).count().collect().await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![100]));

        let expected = RecordBatch::try_from_iter(vec![("count(1 AS count)", a)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_group_pivot() -> Result<(), SparkError> {
        let spark = setup().await;

        let course: ArrayRef = Arc::new(StringArray::from(vec![
            "dotNET", "Java", "dotNET", "dotNET", "Java",
        ]));
        let year: ArrayRef = Arc::new(Int64Array::from(vec![2012, 2012, 2012, 2013, 2013]));
        let earnings: ArrayRef = Arc::new(Int64Array::from(vec![10000, 20000, 5000, 48000, 30000]));

        let data = RecordBatch::try_from_iter(vec![
            ("course", course),
            ("year", year),
            ("earnings", earnings),
        ])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .clone()
            .group_by(Some([col("year")]))
            .pivot("course", Some(vec!["Java"]))
            .sum(["earnings"])
            .collect()
            .await?;

        let year: ArrayRef = Arc::new(Int64Array::from(vec![2012, 2013]));
        let earnings: ArrayRef = Arc::new(Int64Array::from(vec![20000, 30000]));

        let schema = Schema::new(vec![
            Field::new("year", DataType::Int64, false),
            Field::new("Java", DataType::Int64, true),
        ]);

        let expected = RecordBatch::try_new(Arc::new(schema), vec![year, earnings])?;

        assert_eq!(expected, res);

        let res = df
            .group_by(Some([col("year")]))
            .pivot("course", None)
            .sum(["earnings"])
            .collect()
            .await?;

        let year: ArrayRef = Arc::new(Int64Array::from(vec![2012, 2013]));
        let java_earnings: ArrayRef = Arc::new(Int64Array::from(vec![20000, 30000]));
        let dnet_earnings: ArrayRef = Arc::new(Int64Array::from(vec![15000, 48000]));

        let schema = Schema::new(vec![
            Field::new("year", DataType::Int64, false),
            Field::new("Java", DataType::Int64, true),
            Field::new("dotNET", DataType::Int64, true),
        ]);

        let expected =
            RecordBatch::try_new(Arc::new(schema), vec![year, java_earnings, dnet_earnings])?;

        assert_eq!(expected, res);

        Ok(())
    }
}
