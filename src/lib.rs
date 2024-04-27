//! Spark Connection Client for Rust
//!
//! Currently, the Spark Connect client for Rust is **highly experimental** and **should
//! not be used in any production setting**. This is currently a "proof of concept" to identify the methods
//! of interacting with Spark cluster from rust.
//!
//! # Quickstart
//!
//! Create a Spark Session and create a [DataFrame] from a [arrow::array::RecordBatch].
//!
//! ```rust
//! use spark_connect_rs::{SparkSession, SparkSessionBuilder};
//! use spark_connect_rs::functions::{col, lit}
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs")
//!         .build()
//!         .await?;
//!
//!     let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
//!     let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
//!
//!     let data = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?
//!
//!     let df = spark.createDataFrame(&data).await?
//!
//!     // 2 records total
//!     let records = df.select("*")
//!         .withColumn("age_plus", col("age") + lit(4))
//!         .filter(col("name").contains("o"))
//!         .count()
//!         .await?;
//!
//!     Ok(())
//! };
//!```
//!
//! Create a Spark Session and create a DataFrame from a SQL statement:
//!
//! ```rust
//! use spark_connect_rs::{SparkSession, SparkSessionBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs")
//!         .build()
//!         .await?;
//!
//!     let df = spark.sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`").await?;
//!
//!     // Show the first 5 records
//!     df.filter("salary > 3000").show(Some(5), None, None).await?;
//!
//!     Ok(())
//! };
//!```
//!
//! Create a Spark Session, read a CSV file into a DataFrame, apply function transformations, and write the results:
//!
//! ```rust
//! use spark_connect_rs::{SparkSession, SparkSessionBuilder};
//!
//! use spark_connect_rs::functions as F;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs")
//!         .build()
//!         .await?;
//!
//!     let paths = ["/opt/spark/examples/src/main/resources/people.csv"];
//!
//!     let df = spark
//!         .read()
//!         .format("csv")
//!         .option("header", "True")
//!         .option("delimiter", ";")
//!         .load(paths)?;
//!
//!     let df = df
//!         .filter("age > 30")
//!         .select([
//!             F::col("name"),
//!             F::col("age").cast("int")
//!         ]);
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
//! ## Databricks Connection
//!
//! Spark Connect is enabled for Databricks Runtime 13.3 LTS and above, and requires the feature
//! flag `feature = "tls"`. The connection string for the remote session must contain the following
//! values in the string;
//!
//! ```rust
//! "sc://<workspace id>:443/;token=<personal access token>;x-databricks-cluster-id=<cluster-id>"
//! ```
//!
//!

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub mod dataframe;
pub mod plan;
pub mod readwriter;
pub mod session;

pub mod catalog;
mod client;
pub mod column;
pub mod errors;
pub mod expressions;
pub mod functions;
pub mod group;
pub mod storage;
pub mod streaming;
pub mod types;
mod utils;
pub mod window;

pub use dataframe::{DataFrame, DataFrameReader, DataFrameWriter};
pub use session::{SparkSession, SparkSessionBuilder};

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, StringArray},
        record_batch::RecordBatch,
    };

    use crate::errors::SparkError;

    use super::*;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_test;session_id=0d2af2a9-cc3c-4d4b-bf27-e2fefeaca233";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_spark_range() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 100, 1, Some(8));

        let records = df.collect().await?;

        assert_eq!(records.num_rows(), 100);
        Ok(())
    }

    #[tokio::test]
    async fn test_spark_create_dataframe() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));

        let record_batch = RecordBatch::try_from_iter(vec![("a", a)])?;

        let df = spark.createDataFrame(&record_batch)?;

        let rows = df.collect().await?;

        assert_eq!(record_batch, rows);
        Ok(())
    }
}
