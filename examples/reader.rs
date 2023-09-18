use spark_connect_rs;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

// This example demonstrates creating a Spark DataFrame from a CSV with read options
// and then adding transformations for 'select' & 'filter'
// printing the results as "show(...)"
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::default().build().await?;

    let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

    let mut df = spark
        .read()
        .format("csv")
        .option("header", "True")
        .option("delimiter", ";")
        .load(paths);

    df.filter("age > 30")
        .select(vec!["name"])
        .show(Some(5), None, None)
        .await?;

    // print results
    // +-------------+
    // | show_string |
    // +-------------+
    // | +----+      |
    // | |name|      |
    // | +----+      |
    // | |Bob |      |
    // | +----+      |
    // |             |
    // +-------------+

    Ok(())
}
