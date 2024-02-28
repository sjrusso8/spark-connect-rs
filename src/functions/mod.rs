use crate::spark;

use crate::column::Column;
use crate::expressions::ToExpressionVec;

pub fn col(value: &str) -> Column {
    Column::from(value)
}

pub fn column(value: &str) -> Column {
    Column::from(value)
}

pub fn pi() -> Column {
    Column {
        expression: spark::Expression {
            expr_type: Some(spark::expression::ExprType::UnresolvedFunction(
                spark::expression::UnresolvedFunction {
                    function_name: "pi".to_string(),
                    arguments: vec![],
                    is_distinct: false,
                    is_user_defined_function: false,
                },
            )),
        },
    }
}

pub fn coalesce(cols: Vec<Column>) -> Column {
    Column {
        expression: spark::Expression {
            expr_type: Some(spark::expression::ExprType::UnresolvedFunction(
                spark::expression::UnresolvedFunction {
                    function_name: "coalesce".to_string(),
                    arguments: cols.to_expression_vec(),
                    is_distinct: false,
                    is_user_defined_function: false,
                },
            )),
        },
    }
}

#[cfg(test)]
mod tests {

    // use arrow::{
    //     array::Int64Array,
    //     datatypes::{DataType, Field, Schema},
    //     record_batch::RecordBatch,
    // };

    // TODO Update the tests to validate against an arrow dataframe
    use super::*;

    use crate::{SparkSession, SparkSessionBuilder};

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_func".to_string();

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_dataframe_pi() {
        let spark = setup().await;

        let mut df = spark.range(None, 1, 1, Some(1)).select(vec![pi()]);

        df.show(Some(1), None, Some(true)).await.unwrap();

        assert_eq!(100, 100)
    }

    #[tokio::test]
    async fn test_dataframe_select() {
        let spark = setup().await;

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths);

        let value = df
            .select(vec![col("job").alias("role"), col("name")])
            .show(Some(2), None, Some(true))
            .await
            .unwrap();

        println!("{:?}", value);

        assert_eq!(100, 100)
    }

    #[tokio::test]
    async fn test_dataframe_coalesce() {
        let spark = setup().await;

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths);

        let value = df
            .select(vec![
                col("job").alias("role"),
                coalesce(vec![col("name"), col("role")]).alias("new_col"),
            ])
            .show(Some(2), None, Some(true))
            .await
            .unwrap();

        println!("{:?}", value);

        assert_eq!(100, 100)
    }
}
