// This example demonstrates querying an icebery from the Unity Catalog OSS

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002")
        .build()
        .await?;

    // Select from the unity catalog iceberg table
    let df = spark
        .sql("select * from iceberg.unity.default.marksheet_uniform")
        .await?;

    df.show(Some(100), None, None).await?;

    // +------------------------+
    // | show_string            |
    // +------------------------+
    // | +---+----------+-----+ |
    // | |id |name      |marks| |
    // | +---+----------+-----+ |
    // | |1  |nWYHawtqUw|930  | |
    // | |2  |uvOzzthsLV|166  | |
    // | |3  |WIAehuXWkv|170  | |
    // | |4  |wYCSvnJKTo|709  | |
    // | |5  |VsslXsUIDZ|993  | |
    // | |6  |ZLsACYYTFy|813  | |
    // | |7  |BtDDvLeBpK|52   | |
    // | |8  |YISVtrPfGr|8    | |
    // | |9  |PBPJHDFjjC|45   | |
    // | |10 |qbDuUJzJMO|756  | |
    // | |11 |EjqqWoaLJn|712  | |
    // | |12 |jpZLMdKXpn|847  | |
    // | |13 |acpjQXpJCp|649  | |
    // | |14 |nOKqHhRwao|133  | |
    // | |15 |kxUUZEUoKv|398  | |
    // | +---+----------+-----+ |
    // |                        |
    // +------------------------+

    Ok(())
}
