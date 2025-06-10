// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
//!     let df = spark.create_dataframe(&data).await?
//!
//!     // 2 records total
//!     let records = df.select(["*"])
//!         .with_column("age_plus", col("age") + lit(4))
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
//!     let df = spark.sql("SELECT * FROM json.`/datasets/employees.json`").await?;
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
//!     let paths = ["/datasets/people.csv"];
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
//! ```bash
//! "sc://<workspace id>:443/;token=<personal access token>;x-databricks-cluster-id=<cluster-id>"
//! ```
//!
//!

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub mod catalog;
pub mod client;
pub mod column;
pub mod conf;
pub mod dataframe;
pub mod errors;
pub mod expressions;
pub mod functions;
pub mod group;
pub mod plan;
pub mod readwriter;
pub mod session;
pub mod storage;
pub mod streaming;
pub mod types;
pub mod window;

pub use dataframe::{DataFrame, DataFrameReader, DataFrameWriter};
pub use session::{SparkSession, SparkSessionBuilder};
