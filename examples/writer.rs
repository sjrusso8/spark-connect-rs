use std::sync::Arc;

use spark_connect_rs;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use spark_connect_rs::functions::col;

use spark_connect_rs::dataframe::SaveMode;

// This example demonstrates creating a Spark DataFrame from range()
// alias the column name, writing the results to a CSV
// then reading the csv file back
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: Arc<SparkSession> = Arc::new(SparkSessionBuilder::default().build().await?);

    let df = spark
        .clone()
        .range(None, 1000, 1, Some(16))
        .select(col("id").alias("range_id"));

    let path = "/opt/spark/examples/src/main/rust/employees/";

    df.write()
        .format("csv")
        .mode(SaveMode::Overwrite)
        .option("header", "true")
        .save(path)
        .await?;

    let df = spark
        .clone()
        .read()
        .format("csv")
        .option("header", "true")
        .load([path])?;

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
