// This example demonstrates creating a Spark DataFrame from a CSV with read options
// and then adding transformations for 'select' & 'sort'
// The resulting dataframe is saved in the `delta` format as a `managed` table
// and `spark.sql` queries are run against the delta table
//
// The remote spark session must have the spark package `io.delta:delta-spark_2.12:{DELTA_VERSION}` enabled.
// Where the `DELTA_VERSION` is the specified Delta Lake version.

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::remote("sc://<workspace id>:443/;token=<personal access token>;x-databricks-cluster-id=<cluster-id>")
        .build()
        .await?;

    spark
        .read()
        .table("datalake.wine_quality.red", None)
        .limit(5)
        .collect()
        .await
        .unwrap();
    //
    // let df = spark
    //     .range(Some(1), 100, 1, Some(1))
    //     .collect()
    //     .await
    //     .unwrap();

    Ok(())
}
