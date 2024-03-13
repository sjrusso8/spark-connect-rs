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

use spark_connect_rs::functions as F;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/".to_string())
            .build()
            .await?;

    let mut df = spark
        .sql("SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`")
        .await?;

    df.filter("salary >= 3500")
        .select(F::col("name"))
        .show(Some(5), None, None)
        .await?;

    // +-------------+
    // | show_string |
    // +-------------+
    // | +------+    |
    // | |name  |    |
    // | +------+    |
    // | |Andy  |    |
    // | |Justin|    |
    // | |Berta |    |
    // | +------+    |
    // |             |
    // +-------------+

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

The following section outlines some of the larger functionality that
is not yet working with this Spark Connect implementation.

- ![open] TLS authentication & Databricks compatability
- ![open] streaming implementation
- ![open] groupBy, aggregation, and window functions
- ![open] better error handling
- ![open] converting RecordBatch output into a polars DataFrame

### SparkSession

| SparkSession     | API     | Comment                                                                      |
|------------------|---------|------------------------------------------------------------------------------|
| range            | ![done] |                                                                              |
| sql              | ![done] | Does not include the new Spark Connect 3.5 feature with "position arguments" |
| read             | ![done] |                                                                              |
| readStream       | ![open] |                                                                              |
| createDataFrame  | ![open] |                                                                              |
| getActiveSession | ![open] |                                                                              |
| catalog          | ![open] | Partial. List/Get functions are implemented                                  |


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
| dtypes          | ![done] |                                                                              |
| columns         | ![done] |                                                                              |
| schema          | ![done] | The output needs to be handled better                                        |
| explain         | ![done] | The output needs to be handled better                                        |
| show            | ![done] |                                                                              |
| tail            | ![done] |                                                                              |
| collect         | ![done] |                                                                              |
| withColumns     | ![open] |                                                                              |
| sort            | ![done] |                                                                              |
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


| Column           | API     | Comment                                                                      |
|------------------|---------|------------------------------------------------------------------------------|
| alias            | ![done] |                                                                              |
| asc              | ![done] |                                                                              |
| asc_nulls_first  | ![done] |                                                                              |
| asc_nulls_last   | ![done] |                                                                              |
| astype           | ![open] |                                                                              |
| between          | ![open] |                                                                              |
| cast             | ![done] |                                                                              |
| contains         | ![done] |                                                                              |
| desc             | ![done] |                                                                              |
| desc_nulls_first | ![done] |                                                                              |
| desc_nulls_last  | ![done] |                                                                              |
| dropFields       | ![open] |                                                                              |
| endswith         | ![done] |                                                                              |
| ilike            | ![done] |                                                                              |
| isNotNull        | ![done] |                                                                              |
| isNull           | ![done] |                                                                              |
| isin             | ![done] |                                                                              |
| like             | ![done] |                                                                              |
| name             | ![done] |                                                                              |
| otherwise        | ![open] |                                                                              |
| rlike            | ![done] |                                                                              |
| startswith       | ![done] |                                                                              |
| substr           | ![open] |                                                                              |
| when             | ![open] |                                                                              |
| addition `+`     | ![done] |                                                                              |
| subtration `-`   | ![done] |                                                                              |
| multiplication `*` | ![done] |                                                                            |
| division `/`     | ![done] |                                                                              |
| OR `|`           | ![done] |                                                                              |
| AND `&`          | ![done] |                                                                              |
| XOR `^`          | ![done] |                                                                              |
| Negate `~`       | ![done] |                                                                              |


### Functions

Only a few of the functions are covered by unit tests.

| Functions                   | API     | Comment |
|-----------------------------|---------|---------|
| abs                         | ![done] |         |
| acos                        | ![open] |         |
| acosh                       | ![open] |         |
| add_months                  | ![done] |         |
| aggregate                   | ![open] |         |
| approxCountDistinct         | ![open] |         |
| approx_count_distinct       | ![open] |         |
| array                       | ![done] |         |
| array_append                | ![open] |         |
| array_compact               | ![done] |         |
| array_contains              | ![open] |         |
| array_distinct              | ![done] |         |
| array_except                | ![done] |         |
| array_insert                | ![open] |         |
| array_intersect             | ![done] |         |
| array_join                  | ![open] |         |
| array_max                   | ![done] |         |
| array_min                   | ![done] |         |
| array_position              | ![open] |         |
| array_remove                | ![open] |         |
| array_repeat                | ![open] |         |
| array_sort                  | ![open] |         |
| array_union                 | ![done] |         |
| arrays_overlap              | ![open] |         |
| arrays_zip                  | ![done] |         |
| asc                         | ![done] |         |
| asc_nulls_first             | ![done] |         |
| asc_nulls_last              | ![done] |         |
| ascii                       | ![done] |         |
| asin                        | ![open] |         |
| asinh                       | ![open] |         |
| assert_true                 | ![open] |         |
| atan                        | ![open] |         |
| atan2                       | ![open] |         |
| atanh                       | ![open] |         |
| avg                         | ![open] |         |
| base64                      | ![done] |         |
| bin                         | ![done] |         |
| bit_length                  | ![done] |         |
| bitwiseNOT                  | ![open] |         |
| bitwise_not                 | ![done] |         |
| broadcast                   | ![open] |         |
| bround                      | ![open] |         |
| bucket                      | ![open] |         |
| call_udf                    | ![open] |         |
| cast                        | ![open] |         |
| cbrt                        | ![open] |         |
| ceil                        | ![done] |         |
| coalesce                    | ![done] |         |
| col                         | ![done] |         |
| collect_list                | ![open] |         |
| collect_set                 | ![open] |         |
| column                      | ![done] |         |
| concat                      | ![done] |         |
| concat_ws                   | ![open] |         |
| conv                        | ![open] |         |
| corr                        | ![open] |         |
| cos                         | ![open] |         |
| cosh                        | ![open] |         |
| cot                         | ![open] |         |
| count                       | ![open] |         |
| countDistinct               | ![open] |         |
| count_distinct              | ![open] |         |
| covar_pop                   | ![open] |         |
| covar_samp                  | ![open] |         |
| crc32                       | ![done] |         |
| create_map                  | ![done] |         |
| csc                         | ![open] |         |
| cume_dist                   | ![done] |         |
| current_date                | ![done] |         |
| current_timestamp           | ![open] |         |
| date_add                    | ![done] |         |
| date_format                 | ![open] |         |
| date_sub                    | ![done] |         |
| date_trunc                  | ![open] |         |
| datediff                    | ![done] |         |
| dayofmonth                  | ![done] |         |
| dayofweek                   | ![done] |         |
| dayofyear                   | ![done] |         |
| days                        | ![done] |         |
| decode                      | ![open] |         |
| degrees                     | ![open] |         |
| dense_rank                  | ![done] |         |
| desc                        | ![done] |         |
| desc_nulls_first            | ![done] |         |
| desc_nulls_last             | ![done] |         |
| element_at                  | ![open] |         |
| encode                      | ![open] |         |
| exists                      | ![open] |         |
| exp                         | ![done] |         |
| explode                     | ![done] |         |
| explode_outer               | ![done] |         |
| expm1                       | ![open] |         |
| expr                        | ![done] |         |
| factorial                   | ![done] |         |
| filter                      | ![open] |         |
| first                       | ![open] |         |
| flatten                     | ![done] |         |
| floor                       | ![done] |         |
| forall                      | ![open] |         |
| format_number               | ![open] |         |
| format_string               | ![open] |         |
| from_csv                    | ![open] |         |
| from_json                   | ![open] |         |
| from_unixtime               | ![open] |         |
| from_utc_timestamp          | ![open] |         |
| functools                   | ![open] |         |
| get                         | ![open] |         |
| get_active_spark_context    | ![open] |         |
| get_json_object             | ![open] |         |
| greatest                    | ![done] |         |
| grouping                    | ![open] |         |
| grouping_id                 | ![open] |         |
| has_numpy                   | ![open] |         |
| hash                        | ![done] |         |
| hex                         | ![open] |         |
| hour                        | ![done] |         |
| hours                       | ![done] |         |
| hypot                       | ![open] |         |
| initcap                     | ![done] |         |
| inline                      | ![done] |         |
| inline_outer                | ![done] |         |
| input_file_name             | ![done] |         |
| inspect                     | ![open] |         |
| instr                       | ![open] |         |
| isnan                       | ![done] |         |
| isnull                      | ![done] |         |
| json_tuple                  | ![open] |         |
| kurtosis                    | ![open] |         |
| lag                         | ![open] |         |
| last                        | ![open] |         |
| last_day                    | ![open] |         |
| lead                        | ![open] |         |
| least                       | ![done] |         |
| length                      | ![done] |         |
| levenshtein                 | ![open] |         |
| lit                         | ![done] |         |
| localtimestamp              | ![open] |         |
| locate                      | ![open] |         |
| log                         | ![done] |         |
| log10                       | ![done] |         |
| log1p                       | ![done] |         |
| log2                        | ![done] |         |
| lower                       | ![done] |         |
| lpad                        | ![open] |         |
| ltrim                       | ![done] |         |
| make_date                   | ![open] |         |
| map_concat                  | ![done] |         |
| map_contains_key            | ![open] |         |
| map_entries                 | ![done] |         |
| map_filter                  | ![open] |         |
| map_from_arrays             | ![open] |         |
| map_from_entries            | ![done] |         |
| map_keys                    | ![done] |         |
| map_values                  | ![done] |         |
| map_zip_with                | ![open] |         |
| max                         | ![open] |         |
| max_by                      | ![open] |         |
| md5                         | ![done] |         |
| mean                        | ![open] |         |
| median                      | ![open] |         |
| min                         | ![open] |         |
| min_by                      | ![open] |         |
| minute                      | ![done] |         |
| mode                        | ![open] |         |
| monotonically_increasing_id | ![done] |         |
| month                       | ![done] |         |
| months                      | ![done] |         |
| months_between              | ![open] |         |
| nanvl                       | ![done] |         |
| next_day                    | ![open] |         |
| np                          | ![open] |         |
| nth_value                   | ![open] |         |
| ntile                       | ![open] |         |
| octet_length                | ![done] |         |
| overlay                     | ![open] |         |
| overload                    | ![open] |         |
| pandas_udf                  | ![open] |         |
| percent_rank                | ![done] |         |
| percentile_approx           | ![open] |         |
| pmod                        | ![open] |         |
| posexplode                  | ![done] |         |
| posexplode_outer            | ![done] |         |
| pow                         | ![done] |         |
| product                     | ![open] |         |
| quarter                     | ![done] |         |
| radians                     | ![open] |         |
| raise_error                 | ![open] |         |
| rand                        | ![done] |         |
| randn                       | ![done] |         |
| rank                        | ![done] |         |
| regexp_extract              | ![open] |         |
| regexp_replace              | ![open] |         |
| repeat                      | ![open] |         |
| reverse                     | ![done] |         |
| rint                        | ![open] |         |
| round                       | ![done] |         |
| row_number                  | ![done] |         |
| rpad                        | ![open] |         |
| rtrim                       | ![done] |         |
| schema_of_csv               | ![open] |         |
| schema_of_json              | ![open] |         |
| sec                         | ![open] |         |
| second                      | ![done] |         |
| sentences                   | ![open] |         |
| sequence                    | ![open] |         |
| session_window              | ![open] |         |
| sha1                        | ![done] |         |
| sha2                        | ![open] |         |
| shiftLeft                   | ![open] |         |
| shiftRight                  | ![open] |         |
| shiftRightUnsigned          | ![open] |         |
| shiftleft                   | ![open] |         |
| shiftright                  | ![open] |         |
| shiftrightunsigned          | ![open] |         |
| shuffle                     | ![done] |         |
| signum                      | ![open] |         |
| sin                         | ![open] |         |
| sinh                        | ![open] |         |
| size                        | ![done] |         |
| skewness                    | ![open] |         |
| slice                       | ![open] |         |
| sort_array                  | ![open] |         |
| soundex                     | ![done] |         |
| spark_partition_id          | ![done] |         |
| split                       | ![open] |         |
| sqrt                        | ![done] |         |
| stddev                      | ![open] |         |
| stddev_pop                  | ![open] |         |
| stddev_samp                 | ![open] |         |
| struct                      | ![open] |         |
| substring                   | ![open] |         |
| substring_index             | ![open] |         |
| sum                         | ![open] |         |
| sumDistinct                 | ![open] |         |
| sum_distinct                | ![open] |         |
| sys                         | ![open] |         |
| tan                         | ![open] |         |
| tanh                        | ![open] |         |
| timestamp_seconds           | ![done] |         |
| toDegrees                   | ![open] |         |
| toRadians                   | ![open] |         |
| to_csv                      | ![open] |         |
| to_date                     | ![open] |         |
| to_json                     | ![open] |         |
| to_str                      | ![open] |         |
| to_timestamp                | ![open] |         |
| to_utc_timestamp            | ![open] |         |
| transform                   | ![open] |         |
| transform_keys              | ![open] |         |
| transform_values            | ![open] |         |
| translate                   | ![open] |         |
| trim                        | ![done] |         |
| trunc                       | ![open] |         |
| try_remote_functions        | ![open] |         |
| udf                         | ![open] |         |
| unbase64                    | ![done] |         |
| unhex                       | ![open] |         |
| unix_timestamp              | ![open] |         |
| unwrap_udt                  | ![open] |         |
| upper                       | ![done] |         |
| var_pop                     | ![open] |         |
| var_samp                    | ![open] |         |
| variance                    | ![open] |         |
| warnings                    | ![open] |         |
| weekofyear                  | ![done] |         |
| when                        | ![open] |         |
| window                      | ![open] |         |
| window_time                 | ![open] |         |
| xxhash64                    | ![done] |         |
| year                        | ![done] |         |
| years                       | ![done] |         |
| zip_with                    | ![open] |         |

[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
