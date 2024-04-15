// This example demonstrates connecting to a Databricks Cluster via a
// tls connection.
//
// This demo requires access to a Databricks Workspace, a personal access token,
// and a cluster id. The cluster should be running a 13.3LTS runtime or greater. Populate
// the remote URL string between the `<>` with the appropriate details.
//
// To view the connected Spark Session, go to the cluster Spark UI and select the 'Connect' tab.

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark:Arc<SparkSession> = Arc::new(
        SparkSessionBuilder::remote("sc://<workspace id>:443/;token=<personal access token>;x-databricks-cluster-id=<cluster-id>")
        .build()
        .await?
    );

    spark
        .range(None, 10, 1, Some(1))
        .selectExpr(vec!["id * 4"])
        .show(Some(10), None, None)
        .await?;

    // +-------------+
    // | show_string |
    // +-------------+
    // | +--------+  |
    // | |(id * 4)|  |
    // | +--------+  |
    // | |0       |  |
    // | |4       |  |
    // | |8       |  |
    // | |12      |  |
    // | |16      |  |
    // | |20      |  |
    // | |24      |  |
    // | |28      |  |
    // | |32      |  |
    // | |36      |  |
    // | +--------+  |
    // |             |
    // +-------------+

    Ok(())
}
