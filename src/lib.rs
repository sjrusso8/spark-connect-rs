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
mod handler;
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
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

    use super::functions::*;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_test";

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
    async fn test_dataframe_sort() {
        let spark = setup().await;

        let mut df = spark
            .range(None, 100, 1, Some(1))
            .sort(vec![col("id").desc()]);

        let rows = df.limit(1).collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let value = Int64Array::from(vec![99]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(value)]).unwrap();

        assert_eq!(expected_batch, rows[0])
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
            .select(vec![col("name")])
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
            .select(vec![col("range_id")])
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

        df.write()
            .mode(dataframe::SaveMode::Overwrite)
            .saveAsTable("test_table")
            .await
            .unwrap();

        let mut df = spark.clone().read().table("test_table", None);

        let total: usize = df
            .select(vec![col("range_id")])
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();

        assert_eq!(total, 1000)
    }
}
