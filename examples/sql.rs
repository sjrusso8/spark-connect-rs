use spark_connect_rs;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

// This example demonstrates creating a Spark DataFrame from a SQL command
// and then displaying the results as "show(...)"
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut spark: SparkSession =
        SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs".to_string())
            .build()
            .await?;

    let mut df = spark
        .sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`")
        .await;

    df.select("salary").show(Some(5), None, None).await?;

    // +-----------------+
    // | show_string     |
    // +-----------------+
    // | +------+------+ |
    // | |name  |salary| |
    // | +------+------+ |
    // | |Andy  |4500  | |
    // | |Justin|3500  | |
    // | |Berta |4000  | |
    // | +------+------+ |
    // |                 |
    // +-----------------+

    Ok(())
}
