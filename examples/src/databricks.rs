// This example demonstrates connecting to a Databricks Cluster via a tls connection.
//
// This demo requires access to a Databricks Workspace, a personal access token,
// and a cluster id. The cluster should be running a 13.3LTS runtime or greater. Populate
// the remote URL string between the `<>` with the appropriate details.
//
// The Databricks workspace instance name is the same as the Server Hostname value for your cluster.
// Get connection details for a Databricks compute resource via https://docs.databricks.com/en/integrations/compute-details.html
//
// To view the connected Spark Session, go to the cluster Spark UI and select the 'Connect' tab.

use spark_connect_rs::functions::{avg, col};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn_str = "sc://<workspace instance name>:443/;token=<personal access token>;x-databricks-cluster-id=<cluster-id>";

    // connect the databricks cluster
    let spark: SparkSession = SparkSessionBuilder::remote(conn_str).build().await?;

    // read unity catalog table
    let df = spark.read().table("samples.nyctaxi.trips", None)?;

    // apply a filter
    let filter = "trip_distance BETWEEN 0 AND 10 AND fare_amount BETWEEN 0 AND 50";
    let df = df.filter(filter);

    // groupby the pickup
    let df = df
        .select(["pickup_zip", "fare_amount"])
        .group_by(Some(["pickup_zip"]));

    // average the fare amount and order by the top 10 zip codes
    let df = df
        .agg([avg(col("fare_amount")).alias("avg_fare_amount")])
        .order_by([col("avg_fare_amount").desc()]);

    df.show(Some(10), None, None).await?;

    // +---------------------------------+
    // | show_string                     |
    // +---------------------------------+
    // | +----------+------------------+ |
    // | |pickup_zip|avg_fare_amount   | |
    // | +----------+------------------+ |
    // | |7086      |40.0              | |
    // | |7030      |40.0              | |
    // | |11424     |34.25             | |
    // | |7087      |31.0              | |
    // | |10470     |28.0              | |
    // | |11371     |25.532619926199263| |
    // | |11375     |25.5              | |
    // | |11370     |22.452380952380953| |
    // | |11207     |20.5              | |
    // | |11218     |20.0              | |
    // | +----------+------------------+ |
    // | only showing top 10 rows        |
    // |                                 |
    // +---------------------------------+

    Ok(())
}
