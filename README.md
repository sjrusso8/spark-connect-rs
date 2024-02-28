# Apache Spark Connect Client for Rust

This project houses the **experimental** client for [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for
[Apache Spark](https://spark.apache.org/) written in [Rust](https://www.rust-lang.org/)


## Current State of the Project

Currently, the Spark Connect client for Rust is **highly experimental** and **should
not be used in any production setting**. This is currently a "proof of concept" to identify the methods
of interacting with Spark cluster from rust.

## Quick Start

The `spark-connect-rs` aims to provide an entrypoint to [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), and provide *similar* DataFrame API interactions.

```bash
docker compose up --build -d
```

```rust
use spark_connect_rs;

use spark_connect_rs::{SparkSession, SparkSessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSessionBuilder::remote("sc://127.0.0.1:15002/".to_string())
            .build()
            .await?;

    let mut df = spark.sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`");

    df.filter("salary > 3000").show(Some(5), None, None).await?;

    Ok(())
}
```

## Getting Started

```
git clone https://github.com/sjrusso8/spark-connect-rs.git
git submodule update --init --recursive

docker compose up --build -d

cargo build && cargo test
```

## Features

The following section outlines some of the implemented functions that
are working with the Spark Connect session.

- ![open] better Error handling
- ![open] not so much use of `clone()`

### SparkSession

| SparkSession     | API     | Comment                                                                      |
|------------------|---------|------------------------------------------------------------------------------|
| range            | ![done] |                                                                              |
| sql              | ![done] | Does not include the new Spark Connect 3.5 feature with "position arguments" |
| read             | ![done] |                                                                              |
| createDataFrame  | ![open] |                                                                              |
| getActiveSession | ![open] |                                                                              |
| many more!!      |         |                                                                              |


### DataFrame

| DataFrame       | API     | Comment                                                                      |
|-----------------|---------|------------------------------------------------------------------------------|
| select          | ![done] |                                                                              |
| selectExpr      | ![done] | Does not include the new Spark Connect 3.5 feature with "position arguments" |
| filter          | ![done] |                                                                              |
| limit           | ![done] |                                                                              |
| dropDuplicates  | ![done] |                                                                              |
| withColumnsRenamed | ![done] |                                                                           |
| drop            | ![done] |                                                                              |
| sample          | ![done] |                                                                              |
| repartition     | ![done] |                                                                              |
| offset          | ![done] |                                                                              |
| schema          | ![done] | The output needs to be handled better                                        |
| explain         | ![done] | The output needs to be handled better                                        |
| show            | ![done] |                                                                              |
| tail            | ![done] |                                                                              |
| collect         | ![done] |                                                                              |
| withColumns     | ![open] |                                                                              |
| sort            | ![open] |                                                                              |
| groupBy         | ![open] |                                                                              |
| createTempView  | ![open] | There is an error right now, and the functions are private till it's fixed   |
| many more!      | ![open] |                                                                              |

### DataFrameWriter

Spark Connect *should* respect the format as long as your cluster supports the specified type and has the
required jars

| DataFrame       | API     | Comment                                                                      |
|-----------------|---------|------------------------------------------------------------------------------|
| format          | ![done] |                                                                              |
| option          | ![done] |                                                                              |
| options         | ![done] |                                                                              |
| mode            | ![done] |                                                                              |
| bucketBy        | ![done] |                                                                              |
| sortBy          | ![done] |                                                                              |
| partitionBy     | ![done] |                                                                              |
| save            | ![done] |                                                                              |
| saveAsTable     | ![done] |                                                                              |
| insertInto      | ![done] |                                                                              |


### Column

Spark [Column](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html) type object and its implemented traits


| DataFrame        | API     | Comment                                                                      |
|------------------|---------|------------------------------------------------------------------------------|
| alias            | ![done] |                                                                              |
| asc              | ![open] |                                                                              |
| asc_nulls_first  | ![open] |                                                                              |
| asc_nulls_last   | ![open] |                                                                              |
| astype           | ![open] |                                                                              |
| between          | ![open] |                                                                              |
| cast             | ![open] |                                                                              |
| contains         | ![open] |                                                                              |
| desc             | ![open] |                                                                              |
| desc_nulls_first | ![open] |                                                                              |
| desc_nulls_last  | ![open] |                                                                              |
| dropFields       | ![open] |                                                                              |
| endswith         | ![open] |                                                                              |
| ilike            | ![open] |                                                                              |
| isNotNull        | ![open] |                                                                              |
| isNull           | ![open] |                                                                              |
| isin             | ![open] |                                                                              |
| like             | ![open] |                                                                              |
| name             | ![done] |                                                                              |
| otherwise        | ![open] |                                                                              |
| rlike            | ![open] |                                                                              |
| startswith       | ![open] |                                                                              |
| substr           | ![open] |                                                                              |
| when             | ![open] |                                                                              |

[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
