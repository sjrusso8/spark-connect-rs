use spark_connect_rs;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

// This example demonstrates creating a Spark DataFrame from range()
// changing the column name, writing the results to a CSV
// then reading the csv file back
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::default().build().await?;

    let df = spark
        .clone()
        .range(None, 1000, 1, Some(16))
        .selectExpr(vec!["id AS range_id"]);

    let path = "/opt/spark/examples/src/main/rust/employees/";

    df.write()
        .format("csv")
        .option("header", "true")
        .save(path)
        .await?;

    let mut df = spark
        .clone()
        .read()
        .format("csv")
        .option("header", "true")
        .load(vec![path.to_string()]);

    df.show(Some(10), None, None).await?;

    // print results may slighty vary but should be close to the below
    // +--------------------------+
    // | show_string              |
    // +--------------------------+
    // | +--------+               |
    // | |range_id|               |
    // | +--------+               |
    // | |312     |               |
    // | |313     |               |
    // | |314     |               |
    // | |315     |               |
    // | |316     |               |
    // | |317     |               |
    // | |318     |               |
    // | |319     |               |
    // | |320     |               |
    // | |321     |               |
    // | +--------+               |
    // | only showing top 10 rows |
    // |                          |
    // +--------------------------+

    Ok(())
}
