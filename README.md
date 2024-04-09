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
    let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/")
            .build()
            .await?;

    let df = spark
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

The following section outlines some of the larger functionality that are not yet working with this Spark Connect implementation.

- ![done] TLS authentication & Databricks compatability
- ![open] StreamingQueryManager
- ![open] Window and ~~Pivot~~ functions
- ![open] UDFs or any type of functionality that takes a closure (foreach, foreachBatch, etc.)

### SparkSession

[Spark Session](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html) type object and its implemented traits

| SparkSession       | API     | Comment                                       |
|--------------------|---------|-----------------------------------------------|
| active             | ![open] |                                               |
| appName            | ![open] |                                               |
| catalog            | ![open] | Partial. Only Get/List traits are implemented |
| createDataFrame    | ![done] | Partial. Only works for `RecordBatch`         |
| range              | ![done] |                                               |
| read               | ![done] |                                               |
| readStream         | ![done] | Creates a `DataStreamReader` object           |
| sql                | ![done] |                                               |
| stop               | ![open] |                                               |
| streams            | ![open] | Stream Manager is not yet implemented         |
| table              | ![open] |                                               |
| version            | ![open] |                                               |
| addArtifact(s)     | ![open] |                                               |
| interruptAll       | ![open] |                                               |
| interruptTag       | ![open] |                                               |
| interruptOperation | ![open] |                                               |
| addTag             | ![open] |                                               |
| removeTag          | ![open] |                                               |
| getTags            | ![open] |                                               |
| clearTags          | ![open] |                                               |


### DataFrame

Spark [DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) type object and its implemented traits.

| DataFrame                     | API     | Comment                                                    |
|-------------------------------|---------|------------------------------------------------------------|
| agg                           | ![done] |                                                            |
| alias                         | ![done] |                                                            |
| approxQuantile                | ![open] |                                                            |
| cache                         | ![done] |                                                            |
| checkpoint                    | ![open] |                                                            |
| coalesce                      | ![done] |                                                            |
| colRegex                      | ![done] |                                                            |
| collect                       | ![done] |                                                            |
| columns                       | ![done] |                                                            |
| corr                          | ![done] |                                                            |
| count                         | ![done] |                                                            |
| cov                           | ![done] |                                                            |
| createGlobalTempView          | ![done] |                                                            |
| createOrReplaceGlobalTempView | ![done] |                                                            |
| createOrReplaceTempView       | ![done] |                                                            |
| createTempView                | ![done] |                                                            |
| crossJoin                     | ![done] |                                                            |
| crosstab                      | ![done] |                                                            |
| cube                          | ![done] |                                                            |
| describe                      | ![done] |                                                            |
| distinct                      | ![done] |                                                            |
| drop                          | ![done] |                                                            |
| dropDuplicates                | ![done] |                                                            |
| dropDuplicatesWithinWatermark | ![open] | Windowing functions are currently in progress              |
| drop_duplicates               | ![done] |                                                            |
| dropna                        | ![done] |                                                            |
| dtypes                        | ![done] |                                                            |
| exceptAll                     | ![done] |                                                            |
| explain                       | ![done] |                                                            |
| fillna                        | ![open] |                                                            |
| filter                        | ![done] |                                                            |
| first                         | ![done] |                                                            |
| foreach                       | ![open] |                                                            |
| foreachPartition              | ![open] |                                                            |
| freqItems                     | ![done] |                                                            |
| groupBy                       | ![done] |                                                            |
| head                          | ![done] |                                                            |
| hint                          | ![done] |                                                            |
| inputFiles                    | ![done] |                                                            |
| intersect                     | ![done] |                                                            |
| intersectAll                  | ![done] |                                                            |
| isEmpty                       | ![done] |                                                            |
| isLocal                       | ![open] |                                                            |
| isStreaming                   | ![done] |                                                            |
| join                          | ![done] |                                                            |
| limit                         | ![done] |                                                            |
| localCheckpoint               | ![open] |                                                            |
| mapInPandas                   | ![open] | TBD on this exact implementation                           |
| mapInArrow                    | ![open] | TBD on this exact implementation                           |
| melt                          | ![done] |                                                            |
| na                            | ![open] |                                                            |
| observe                       | ![open] |                                                            |
| offset                        | ![done] |                                                            |
| orderBy                       | ![done] |                                                            |
| persist                       | ![done] |                                                            |
| printSchema                   | ![done] |                                                            |
| randomSplit                   | ![open] |                                                            |
| registerTempTable             | ![open] |                                                            |
| repartition                   | ![done] |                                                            |
| repartitionByRange            | ![open] |                                                            |
| replace                       | ![open] |                                                            |
| rollup                        | ![done] |                                                            |
| sameSemantics                 | ![done] |                                                            |
| sample                        | ![done] |                                                            |
| sampleBy                      | ![open] |                                                            |
| schema                        | ![done] |                                                            |
| select                        | ![done] |                                                            |
| selectExpr                    | ![done] |                                                            |
| semanticHash                  | ![done] |                                                            |
| show                          | ![done] |                                                            |
| sort                          | ![done] |                                                            |
| sortWithinPartitions          | ![done] |                                                            |
| sparkSession                  | ![done] |                                                            |
| stat                          | ![done] |                                                            |
| storageLevel                  | ![done] |                                                            |
| subtract                      | ![done] |                                                            |
| summary                       | ![open] |                                                            |
| tail                          | ![done] |                                                            |
| take                          | ![done] |                                                            |
| to                            | ![done] |                                                            |
| toDF                          | ![done] |                                                            |
| toJSON                        | ![open] |                                                            |
| toLocalIterator               | ![open] |                                                            |
| toPandas                      | ![open] | TBD on this exact implementation. Might be toPolars        |
| transform                     | ![open] |                                                            |
| union                         | ![done] |                                                            |
| unionAll                      | ![done] |                                                            |
| unionByName                   | ![done] |                                                            |
| unpersist                     | ![done] |                                                            |
| unpivot                       | ![open] |                                                            |
| where                         | ![done] | use `sort` where is a keyword for rust                     |
| withColumn                    | ![done] |                                                            |
| withColumns                   | ![done] |                                                            |
| withColumnRenamed             | ![open] |                                                            |
| withColumnsRenamed            | ![done] |                                                            |
| withMetadata                  | ![open] |                                                            |
| withWatermark                 | ![open] |                                                            |
| write                         | ![done] |                                                            |
| writeStream                   | ![done] |                                                            |
| writeTo                       | ![open] |                                                            |

### DataFrameWriter

Spark Connect *should* respect the format as long as your cluster supports the specified type and has the
required jars

| DataFrameWriter | API     | Comment                                                                      |
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

### DataStreamWriter

Start a streaming job and return a `StreamingQuery` object to handle the stream operations.

| DataStreamWriter | API     | Comment                                                 |
|-----------------|---------|----------------------------------------------------------|
| format          | ![done] |                                                          |
| foreach         | ![open] |                                                          |
| foreachBatch    | ![open] |                                                          |
| option          | ![done] |                                                          |
| options         | ![done] |                                                          |
| outputMode      | ![done] | Uses an Enum for `OutputMode`                            |
| partitionBy     | ![done] |                                                          |
| queryName       | ![done] |                                                          |
| trigger         | ![done] | Uses an Enum for `TriggerMode`                           |
| start           | ![done] |                                                          |
| toTable         | ![done] |                                                          |

### StreamingQuery

A handle to a query that is executing continuously in the background as new data arrives.

| StreamingQuery      | API     | Comment |
|---------------------|---------|---------|
| awaitTermination    | ![done] |         |
| exception           | ![open] |         |
| explain             | ![open] |         |
| processAllAvailable | ![open] |         |
| stop                | ![done] |         |
| id                  | ![done] |         |
| isActive            | ![done] |         |
| lastProgress        | ![done] |         |
| name                | ![done] |         |
| recentProgress      | ![done] |         |
| runId               | ![done] |         |
| status              | ![done] |         |

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
| eq `==`          | ![done] | Rust does not like when you try to overload `==` and return something other than a `bool`. Currently implemented column equality like `col('name').eq(col('id'))`. Not the best, but it works for now                                                                           |
| addition `+`     | ![done] |                                                                              |
| subtration `-`   | ![done] |                                                                              |
| multiplication `*` | ![done] |                                                                            |
| division `/`     | ![done] |                                                                              |
| OR  `\|`         | ![done] |                                                                              |
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


### Schema

Spark schema objects have not yet been translated into rust objects.


### Literal Types

Create Spark literal types from these rust types. E.g. `lit(1_i64)` would be a `LongType()` in the schema.

An array can be made like `lit([1_i16,2_i16,3_i16])` would result in an `ArrayType(Short)` since all the values of the slice can be translated into literal type.

| Spark Literal Type | Rust Type           | Status  |
|--------------------|---------------------|---------|
| Null               |                     | ![open] |
| Binary             |                     | ![open] |
| Boolean            | `bool`              | ![done] |
| Byte               |                     | ![open] |
| Short              | `i16`               | ![done] |
| Integer            | `i32`               | ![done] |
| Long               | `i64`               | ![done] |
| Float              | `f32`               | ![done] |
| Double             | `f64`               | ![done] |
| Decimal            |                     | ![open] |
| String             | `&str` / `String`   | ![done] |
| Date               | `chrono::NaiveDate` | ![done] |
| Timestamp          |                     | ![open] |
| TimestampNtz       | `chrono::TimeZone`  | ![done] |
| CalendarInterval   |                     | ![open] |
| YearMonthInterval  |                     | ![open] |
| DayTimeInterval    |                     | ![open] |
| Array              | `slice` / `Vec`     | ![done] |
| Map                |                     | ![open] |
| Struct             |                     | ![open] |


[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
