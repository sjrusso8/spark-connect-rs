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

use spark_connect_rs::streaming::{OutputMode, Trigger};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use std::{thread, time};

// This example demonstrates creating a Spark Stream and monitoring the progress
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession =
        SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=stream_example")
            .build()
            .await?;

    let df = spark
        .read_stream()
        .format("rate")
        .option("rowsPerSecond", "5")
        .load(None)?;

    let query = df
        .write_stream()
        .format("console")
        .query_name("example_stream")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::ProcessingTimeInterval("1 seconds".to_string()))
        .start(None)
        .await?;

    // loop to get multiple progression stats
    for _ in 1..5 {
        thread::sleep(time::Duration::from_secs(5));
        let val = query.last_progress().await?;
        println!("{}", val);
    }

    // stop the active stream
    query.stop().await?;

    Ok(())
}
