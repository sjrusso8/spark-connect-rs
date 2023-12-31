//! Spark Connection Client for Rust
//!
//! Currently, the Spark Connect client for Rust is **highly experimental** and **should
//! not be used in any production setting**. This is currently a "proof of concept" to identify the methods
//! of interacting with Spark cluster from rust.
//!
//! # Usage
//!
//! Create a Spark Session and create a DataFrame from a SQL statement:
//!
//! ```rust
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs".to_string())
//!         .build()
//!         .await?;
//!
//!     let mut df = spark.sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`");
//!
//!     df.filter("salary > 3000").show(Some(5), None, None).await?;
//!
//!     Ok(())
//! };
//!```
//!
//! Create a Spark Session, create a DataFrame from a CSV file, and write the results:
//!
//! ```rust
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs".to_string())
//!         .build()
//!         .await?;
//!
//!     let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];
//!
//!     let mut df = spark
//!         .read()
//!         .format("csv")
//!         .option("header", "True")
//!         .option("delimiter", ";")
//!         .load(paths);
//!
//!     let mut df = df
//!         .filter("age > 30")
//!         .select(vec!["name"]);
//!
//!     df.write()
//!       .format("csv")
//!       .option("header", "true")
//!       .save("/opt/spark/examples/src/main/rust/people/")
//!       .await?;
//!
//!     Ok(())
//! };
//!```
//!

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub mod dataframe;
pub mod execution;
pub mod plan;

pub use arrow;
pub use dataframe::{DataFrame, DataFrameReader, DataFrameWriter};
pub use execution::context::{SparkSession, SparkSessionBuilder};
pub use plan::LogicalPlanBuilder;

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_test".to_string();

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_dataframe_range() {
        let spark = setup().await;

        let mut df = spark.range(None, 100, 1, Some(8));

        let rows = df.collect().await.unwrap();

        let total: usize = rows.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(total, 100)
    }

    #[tokio::test]
    async fn test_dataframe_read() {
        let spark = setup().await;

        let paths = vec!["/opt/spark/examples/src/main/resources/people.csv".to_string()];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(paths);

        let rows = df
            .filter("age > 30")
            .select(vec!["name"])
            .collect()
            .await
            .unwrap();

        assert_eq!(rows[0].num_rows(), 1);
        // assert_eq!(rows[0].column(0).);
    }

    #[tokio::test]
    async fn test_dataframe_write() {
        let spark = setup().await;

        let df = spark
            .clone()
            .range(None, 1000, 1, Some(16))
            .selectExpr(vec!["id AS range_id"]);

        let path = "/opt/spark/examples/src/main/rust/employees/";

        df.write()
            .format("csv")
            .option("header", "true")
            .save(path)
            .await
            .unwrap();

        let mut df = spark
            .clone()
            .read()
            .format("csv")
            .option("header", "true")
            .load(vec![path.to_string()]);

        let total: usize = df
            .select(vec!["range_id"])
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();

        assert_eq!(total, 1000)
    }

    #[tokio::test]
    async fn test_dataframe_write_table() {
        let spark = setup().await;

        let df = spark
            .clone()
            .range(None, 1000, 1, Some(16))
            .selectExpr(vec!["id AS range_id"]);

        df.write().saveAsTable("test_table").await.unwrap();

        let mut df = spark.clone().read().table("test_table", None);

        let total: usize = df
            .select(vec!["range_id"])
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();

        assert_eq!(total, 1000)
    }
}
