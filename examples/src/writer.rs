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

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use spark_connect_rs::functions::col;

use spark_connect_rs::dataframe::SaveMode;

// This example demonstrates creating a Spark DataFrame from range()
// alias the column name, writing the results to a CSV
// then reading the csv file back
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/")
        .build()
        .await?;

    let df = spark
        .range(None, 1000, 1, Some(16))
        .select([col("id").alias("range_id")]);

    let path = "file:///tmp/range_table/";

    df.write()
        .format("csv")
        .mode(SaveMode::Overwrite)
        .option("header", "true")
        .save(path)
        .await?;

    let df = spark
        .read()
        .format("csv")
        .option("header", "true")
        .load([path])?;

    df.show(Some(10), None, None).await?;

    // print results may slighty vary but should be close to the below
    // +--------------------------+
    // | show_string              |
    // +--------------------------+
    // | +--------+               |
    // | |range_id|               |
    // | +--------+               |
    // | |312     |               |
    // | |313     |               |
    // | |314     |               |
    // | |315     |               |
    // | |316     |               |
    // | |317     |               |
    // | |318     |               |
    // | |319     |               |
    // | |320     |               |
    // | |321     |               |
    // | +--------+               |
    // | only showing top 10 rows |
    // |                          |
    // +--------------------------+

    Ok(())
}
