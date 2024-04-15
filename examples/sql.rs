use std::sync::Arc;

use spark_connect_rs;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

// This example demonstrates creating a Spark DataFrame from a SQL command
// and leveraging &str input for `filter` and `select` to change the dataframe
// Displaying the results as "show(...)"
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: Arc<SparkSession> = Arc::new(
        SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs")
            .build()
            .await?,
    );

    let df = spark
        .sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`")
        .await?;

    df.filter("salary >= 3500")
        .select("*")
        .show(Some(5), None, None)
        .await?;

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
