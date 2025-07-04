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

// This example demonstrates creating a Spark DataFrame from a CSV with read options
// and then adding transformations for 'select' & 'sort'
// The resulting dataframe is saved in the `delta` format as a `managed` table
// and `spark.sql` queries are run against the delta table
//
// The remote spark session must have the spark package `io.delta:delta-spark_2.12:{DELTA_VERSION}` enabled.
// Where the `DELTA_VERSION` is the specified Delta Lake version.

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use spark_connect_rs::dataframe::SaveMode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/")
        .build()
        .await?;

    // path might vary based on where you started your spark cluster
    // the `/datasets/` folder of spark contains dummy data
    let paths = ["./datasets/people.csv"];

    // Load a CSV file from the spark server
    let df = spark
        .read()
        .format("csv")
        .option("header", "True")
        .option("delimiter", ";")
        .option("inferSchema", "True")
        .load(paths)?;

    // write as a delta table and register it as a table
    df.write()
        .format("delta")
        .mode(SaveMode::Overwrite)
        .save_as_table("default.people_delta")
        .await?;

    // view the history of the table
    spark
        .sql("DESCRIBE HISTORY default.people_delta")
        .await?
        .show(Some(1), None, Some(true))
        .await?;

    // create another dataframe
    let df = spark
        .sql("SELECT 'john' as name, 40 as age, 'engineer' as job")
        .await?;

    // append to the delta table
    df.write()
        .format("delta")
        .mode(SaveMode::Append)
        .save_as_table("default.people_delta")
        .await?;

    // view history
    spark
        .sql("DESCRIBE HISTORY default.people_delta")
        .await?
        .show(Some(2), None, Some(true))
        .await?;

    // +-------------------------------------------------------------------------------------------------------+
    // | show_string                                                                                           |
    // +-------------------------------------------------------------------------------------------------------+
    // | -RECORD 0-------------------------------------------------------------------------------------------- |
    // |  version             | 1                                                                              |
    // |  timestamp           | 2024-05-17 14:27:34.462                                                        |
    // |  userId              | NULL                                                                           |
    // |  userName            | NULL                                                                           |
    // |  operation           | WRITE                                                                          |
    // |  operationParameters | {mode -> Append, partitionBy -> []}                                            |
    // |  job                 | NULL                                                                           |
    // |  notebook            | NULL                                                                           |
    // |  clusterId           | NULL                                                                           |
    // |  readVersion         | 0                                                                              |
    // |  isolationLevel      | Serializable                                                                   |
    // |  isBlindAppend       | true                                                                           |
    // |  operationMetrics    | {numFiles -> 1, numOutputRows -> 1, numOutputBytes -> 947}                     |
    // |  userMetadata        | NULL                                                                           |
    // |  engineInfo          | Apache-Spark/3.5.1 Delta-Lake/3.0.0                                            |
    // | -RECORD 1-------------------------------------------------------------------------------------------- |
    // |  version             | 0                                                                              |
    // |  timestamp           | 2024-05-17 14:27:30.726                                                        |
    // |  userId              | NULL                                                                           |
    // |  userName            | NULL                                                                           |
    // |  operation           | CREATE OR REPLACE TABLE AS SELECT                                              |
    // |  operationParameters | {isManaged -> true, description -> NULL, partitionBy -> [], properties -> {}}  |
    // |  job                 | NULL                                                                           |
    // |  notebook            | NULL                                                                           |
    // |  clusterId           | NULL                                                                           |
    // |  readVersion         | NULL                                                                           |
    // |  isolationLevel      | Serializable                                                                   |
    // |  isBlindAppend       | false                                                                          |
    // |  operationMetrics    | {numFiles -> 1, numOutputRows -> 2, numOutputBytes -> 988}                     |
    // |  userMetadata        | NULL                                                                           |
    // |  engineInfo          | Apache-Spark/3.5.1 Delta-Lake/3.0.0                                            |
    // |                                                                                                       |
    // +-------------------------------------------------------------------------------------------------------+

    Ok(())
}
