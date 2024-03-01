use spark_connect_rs;

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
        F::col("age"),
        F::pow(F::col("age").cast("int"), F::lit(3_i32)),
        F::pi().alias("pi"),
        (F::col("age") + F::pi()).alias("sum"),
        F::monotonically_increasing_id(),
        F::rand(Some(5)),
        F::spark_partition_id(),
        F::lit(3.0),
    ])
    .show(Some(5), None, None)
    .await?;

    // df.select(vec![col("name"), lit(1_i32), lit(true)])
    //     .filter(col("name").contains("o"))
    //     .show(Some(5), None, None)
    //     .await?;

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
