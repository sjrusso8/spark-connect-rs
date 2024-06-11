// This example demonstrates creating a Spark DataFrame from a CSV with read options
// and then adding transformations for 'select' & 'sort'
// printing the results as "show(...)"

use spark_connect_rs::functions as F;
use spark_connect_rs::types::DataType;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: Arc<SparkSession> = Arc::new(SparkSessionBuilder::default().build().await?);

    let path = "./datasets/people.csv";

    let df = spark
        .read()
        .format("csv")
        .option("header", "True")
        .option("delimiter", ";")
        .load([path])?;

    // select columns and perform data manipulations
    let df = df
        .select([
            F::col("name"),
            F::col("age").cast(DataType::Integer).alias("age_int"),
            (F::lit(3.0) + F::col("age_int")).alias("addition"),
        ])
        .sort([F::col("name").desc()]);

    df.show(Some(5), None, None).await?;

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
