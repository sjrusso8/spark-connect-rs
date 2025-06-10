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

// This example demonstrates creating a Spark DataFrame from a SQL command
// and saving the results as a parquet and reading the new parquet file

use spark_connect_rs::dataframe::SaveMode;
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/")
        .build()
        .await?;

    let df = spark.sql("select 'apple' as word, 123 as count").await?;

    df.write()
        .mode(SaveMode::Overwrite)
        .format("parquet")
        .save("file:///tmp/spark-connect-write-example-output.parquet")
        .await?;

    let df = spark
        .read()
        .format("parquet")
        .load(["file:///tmp/spark-connect-write-example-output.parquet"])?;

    df.show(Some(100), None, None).await?;

    // +---------------+
    // | show_string   |
    // +---------------+
    // | +-----+-----+ |
    // | |word |count| |
    // | +-----+-----+ |
    // | |apple|123  | |
    // | +-----+-----+ |
    // |               |
    // +---------------+

    Ok(())
}
