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

use chrono::{offset, DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};
use spark_connect_rs::dataframe::ExplainMode;
use spark_connect_rs::functions as F;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::default().build().await?;

    let dt = Local::now();
    let naive_utc = dt.naive_utc();
    let offset = dt.offset().clone();
    let dt_new = DateTime::<Local>::from_naive_utc_and_offset(naive_utc, offset);

    let dt: NaiveDateTime = NaiveDate::from_ymd_opt(2016, 7, 8)
        .unwrap()
        .and_hms_opt(9, 10, 11)
        .unwrap();

    println!("{:?}", &dt_new);
    println!("{:?}", &dt);

    let value = "Hello World".as_bytes();

    let df = spark
        .range(None, 10, 1, Some(1))
        .select([
            F::lit(value).alias("bytes"),
            F::lit(dt_new),
            F::lit(dt),
            F::current_timestamp(),
            F::current_timezone(),
        ])
        .selectExpr(["decode(bytes, 'UTF-8') AS STRING"]);

    df.clone().explain(Some(ExplainMode::Extended)).await?;

    df.show(Some(3), None, None).await?;

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
