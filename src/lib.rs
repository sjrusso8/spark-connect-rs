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
//! use spark_connect_rs::{SparkSession, SparkSessionBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs")
//!         .build()
//!         .await?;
//!
//!     let mut df = spark.sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`").await?;
//!
//!     df.filter("salary > 3000").show(Some(5), None, None).await?;
//!
//!     Ok(())
//! };
//!```
//!
//! Create a Spark Session, create a DataFrame from a CSV file, apply function transformations, and write the results:
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
//!         .select(vec![
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

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub mod dataframe;
pub mod plan;
pub mod readwriter;
pub mod session;

mod catalog;
mod client;
pub mod column;
mod errors;
mod expressions;
pub mod functions;
mod group;
pub mod storage;
mod types;
mod utils;

pub use arrow;
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
