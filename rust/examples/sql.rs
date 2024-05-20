// This example demonstrates creating a Spark DataFrame from a SQL command
// and saving the results as a parquet and reading the new parquet file

use spark_connect_rs::dataframe::SaveMode;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: Arc<SparkSession> = Arc::new(
        SparkSessionBuilder::remote("sc://127.0.0.1:15002/")
            .build()
            .await?,
    );

    let df = spark
        .clone()
        .sql("select 'apple' as word, 123 as count")
        .await?;

    df.write()
        .mode(SaveMode::Overwrite)
        .format("parquet")
        .save("file:///tmp/spark-connect-write-example-output.parquet")
        .await?;

    let df = spark
        .read()
        .format("parquet")
        .load(["file:///tmp/spark-connect-write-example-output.parquet"])?;

    df.show(Some(100), None, None).await?;

    // +---------------+
    // | show_string   |
    // +---------------+
    // | +-----+-----+ |
    // | |word |count| |
    // | +-----+-----+ |
    // | |apple|123  | |
    // | +-----+-----+ |
    // |               |
    // +---------------+

    Ok(())
}
