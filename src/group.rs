//! A DataFrame created with an aggregate statement

use crate::dataframe::DataFrame;
use crate::expressions::ToVecExpr;
use crate::plan::LogicalPlanBuilder;

use crate::functions::lit;
use crate::utils::invoke_func;

use crate::spark;
use crate::spark::aggregate::GroupType;

#[derive(Clone, Debug)]
pub struct GroupedData {
    df: DataFrame,
    group_type: GroupType,
    grouping_cols: Vec<spark::Expression>,
}

impl GroupedData {
    pub fn new(
        df: DataFrame,
        group_type: GroupType,
        grouping_cols: Vec<spark::Expression>,
    ) -> GroupedData {
        Self {
            df,
            group_type,
            grouping_cols,
        }
    }

    pub fn agg<T: ToVecExpr>(self, exprs: T) -> DataFrame {
        let logical_plan = LogicalPlanBuilder::aggregate(
            self.df.logical_plan,
            self.group_type,
            self.grouping_cols,
            exprs,
        );

        DataFrame::new(self.df.spark_session, logical_plan)
    }

    pub fn avg<T: ToVecExpr>(self, cols: T) -> DataFrame {
        self.agg(invoke_func("avg", cols))
    }

    pub fn min<T: ToVecExpr>(self, cols: T) -> DataFrame {
        self.agg(invoke_func("min", cols))
    }

    pub fn max<T: ToVecExpr>(self, cols: T) -> DataFrame {
        self.agg(invoke_func("max", cols))
    }

    pub fn sum<T: ToVecExpr>(self, cols: T) -> DataFrame {
        self.agg(invoke_func("sum", cols))
    }

    pub fn count(self) -> DataFrame {
        self.agg(invoke_func("count", lit(1).alias("count")))
    }
}

#[cfg(test)]
mod tests {

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::errors::SparkError;
    use crate::SparkSession;
    use crate::SparkSessionBuilder;

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

        let path = ["/opt/spark/examples/src/main/resources/people.csv"];

        let df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path)?;

        let res = df.groupBy::<Column>(None).count().collect().await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![2]));

        let record_batch = RecordBatch::try_from_iter(vec![("count(1 AS count)", a)])?;

        assert_eq!(record_batch, res);
        Ok(())
    }
}
