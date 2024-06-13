use spark_connect_rs::streaming::{OutputMode, Trigger};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use std::{thread, time};

// This example demonstrates creating a Spark Stream and monitoring the progress
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession =
        SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=stream_example")
            .build()
            .await?;

    let df = spark
        .read_stream()
        .format("rate")
        .option("rowsPerSecond", "5")
        .load(None)?;

    let query = df
        .write_stream()
        .format("console")
        .query_name("example_stream")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::ProcessingTimeInterval("1 seconds".to_string()))
        .start(None)
        .await?;

    // loop to get multiple progression stats
    for _ in 1..5 {
        thread::sleep(time::Duration::from_secs(5));
        let val = &query.clone().last_progress().await?;
        println!("{}", val);
    }

    // stop the active stream
    query.stop().await?;

    Ok(())
}
