// This example demonstrates creating a Spark DataFrame from a CSV with read options
// and then adding transformations for 'select' & 'sort'
// The resulting dataframe is saved in the `delta` format as a `managed` table
// and `spark.sql` queries are run against the delta table
//
// The remote spark session must have the spark package `io.delta:delta-spark_2.12:{DELTA_VERSION}` enabled.
// Where the `DELTA_VERSION` is the specified Delta Lake version.

use std::sync::Arc;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use spark_connect_rs::dataframe::SaveMode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: Arc<SparkSession> = Arc::new(SparkSessionBuilder::default().build().await?);

    let paths = ["/opt/spark/examples/src/main/resources/people.csv"];

    let df = spark
        .clone()
        .read()
        .format("csv")
        .option("header", "True")
        .option("delimiter", ";")
        .option("inferSchema", "True")
        .load(paths)?;

    df.write()
        .format("delta")
        .mode(SaveMode::Overwrite)
        .saveAsTable("default.people_delta")
        .await?;

    spark
        .sql("DESCRIBE HISTORY default.people_delta")
        .await?
        .show(Some(1), None, Some(true))
        .await?;

    // print results
    // +-------------------------------------------------------------------------------------------------------+
    // | show_string                                                                                           |
    // +-------------------------------------------------------------------------------------------------------+
    // | -RECORD 0-------------------------------------------------------------------------------------------- |
    // |  version             | 3                                                                              |
    // |  timestamp           | 2024-03-16 13:46:23.552                                                        |
    // |  userId              | NULL                                                                           |
    // |  userName            | NULL                                                                           |
    // |  operation           | CREATE OR REPLACE TABLE AS SELECT                                              |
    // |  operationParameters | {isManaged -> true, description -> NULL, partitionBy -> [], properties -> {}}  |
    // |  job                 | NULL                                                                           |
    // |  notebook            | NULL                                                                           |
    // |  clusterId           | NULL                                                                           |
    // |  readVersion         | 2                                                                              |
    // |  isolationLevel      | Serializable                                                                   |
    // |  isBlindAppend       | false                                                                          |
    // |  operationMetrics    | {numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 988}                     |
    // |  userMetadata        | NULL                                                                           |
    // |  engineInfo          | Apache-Spark/3.5.0 Delta-Lake/3.0.0                                            |
    // | only showing top 1 row                                                                                |
    // |                                                                                                       |
    // +-------------------------------------------------------------------------------------------------------+

    Ok(())
}
