use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use spark_connect_rs::functions as F;

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

    df.select(vec![
        F::col("name"),
        F::col("age").cast("int").alias("age_int"),
        (F::lit(3.0) + F::col("age").cast("int")).alias("addition"),
    ])
    .sort(vec![F::col("name").desc()])
    .show(Some(5), None, None)
    .await?;

    // print results
    // +--------------------------+
    // | show_string              |
    // +--------------------------+
    // | +-----+-------+--------+ |
    // | |name |age_int|addition| |
    // | +-----+-------+--------+ |
    // | |Jorge|30     |33.0    | |
    // | |Bob  |32     |35.0    | |
    // | +-----+-------+--------+ |
    // |                          |
    // +--------------------------+

    Ok(())
}
