# Apache Spark Connect Client for Rust

This project houses the **experimental** client for [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for
[Apache Spark](https://spark.apache.org/) written in [Rust](https://www.rust-lang.org/)


## Current State of the Project

Currently, the Spark Connect client for Rust is **highly experimental** and **should
not be used in any production setting**. This is currently a "proof of concept" to identify the methods
of interacting with Spark cluster from rust.

The `spark-connect-rs` aims to provide an entrypoint to [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), and provide *similar* DataFrame API interactions.

## Project Layout

```
├── crates          <- crates for the implementation of the client side spark-connect bindings
│   └─ connect      <- crate for 'spark-connect-rs'
│      └─ protobuf  <- connect protobuf for apache/spark
├── examples        <- examples of using different aspects of the crate
├── datasets        <- sample files from the main spark repo
```

Future state would be to have additional crates that allow for easier creation of other language bindings.

## Getting Started

This section explains how run Spark Connect Rust locally starting from 0.

**Step 1**: Install rust via rustup: https://www.rust-lang.org/tools/install

**Step 2**: Ensure you have a [cmake](https://cmake.org/download/) and [protobuf](https://grpc.io/docs/protoc-installation/) installed on your machine

**Step 3**: Run the following commands to clone the repo

```bash
git clone https://github.com/sjrusso8/spark-connect-rs.git

cargo build
```

**Step 4**: Setup the Spark Driver on localhost either by downloading spark or with [docker](https://docs.docker.com/engine/install/).

With local spark:

1. [Download Spark distribution](https://spark.apache.org/downloads.html) (3.5.1 recommended), unzip the package.

2. Set your `SPARK_HOME` environment variable to the location where spark was extracted to,

2. Start the Spark Connect server with the following command (make sure to use a package version that matches your Spark distribution):

```bash
$ $SPARK_HOME/sbin/start-connect-server.sh --packages "org.apache.spark:spark-connect_2.12:3.5.1,io.delta:delta-spark_2.12:3.0.0" \
      --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
      --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

With docker:

1. Start the Spark Connect server by leveraging the created `docker-compose.yml` in this repo. This will start a Spark Connect Server running on port 15002

```bash
$ docker compose up --build -d
```

**Step 5**: Run an example from the repo under [/examples](https://github.com/sjrusso8/spark-connect-rs/tree/main/examples/README.md)

## Features

The following section outlines some of the larger functionality that are not yet working with this Spark Connect implementation.

- ![done] TLS authentication & Databricks compatability via the feature flag `feature = 'tls'`
- ![open] UDFs or any type of functionality that takes a closure (foreach, foreachBatch, etc.)

### SparkSession

[Spark Session](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html) type object and its implemented traits

|SparkSession      |API       |Comment                                |
|------------------|----------|---------------------------------------|
|active            |![open]   |                                       |
|addArtifact(s)    |![open]   |                                       |
|addTag            |![done]   |                                       |
|clearTags         |![done]   |                                       |
|copyFromLocalToFs |![open]   |                                       |
|createDataFrame   |![partial]|Partial. Only works for `RecordBatch`  |
|getActiveSessions |![open]   |                                       |
|getTags           |![done]   |                                       |
|interruptAll      |![done]   |                                       |
|interruptOperation|![done]   |                                       |
|interruptTag      |![done]   |                                       |
|newSession        |![open]   |                                       |
|range             |![done]   |                                       |
|removeTag         |![done]   |                                       |
|sql               |![done]   |                                       |
|stop              |![open]   |                                       |
|table             |![done]   |                                       |
|catalog           |![done]   |[Catalog](#catalog)                    |
|client            |![done]   |unstable developer api for testing only |
|conf              |![done]   |[Conf](#runtimeconfig)                 |
|read              |![done]   |[DataFrameReader](#dataframereader)    |
|readStream        |![done]   |[DataStreamReader](#datastreamreader)  |
|streams           |![done]   |[Streams](#streamingquerymanager)      |
|udf               |![open]   |[Udf](#udfregistration) - may not be possible   |
|udtf              |![open]   |[Udtf](#udtfregistration) - may not be possible |
|version           |![done]   |                                       |

### SparkSessionBuilder
|SparkSessionBuilder|API       |Comment                                |
|-------------------|----------|---------------------------------------|
|appName            |![done]   |                                       |
|config             |![done]   |                                       |
|master             |![open]   |                                       |
|remote             |![partial]|Validate using [spark connection string](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)|


### RuntimeConfig

|RuntimeConfig     |API       |Comment                                |
|------------------|----------|---------------------------------------|
|get               |![done]   |                                       |
|isModifiable      |![done]   |                                       |
|set               |![done]   |                                       |
|unset             |![done]   |                                       |


### Catalog

|Catalog           |API       |Comment                                |
|------------------|----------|---------------------------------------|
|cacheTable        |![done]   |                                       |
|clearCache        |![done]   |                                       |
|createExternalTale|![done]   |                                       |
|createTable       |![done]   |                                       |
|currentCatalog    |![done]   |                                       |
|currentDatabase   |![done]   |                                       |
|databaseExists    |![done]   |                                       |
|dropGlobalTempView|![done]   |                                       |
|dropTempView      |![done]   |                                       |
|functionExists    |![done]   |                                       |
|getDatabase       |![done]   |                                       |
|getFunction       |![done]   |                                       |
|getTable          |![done]   |                                       |
|isCached          |![done]   |                                       |
|listCatalogs      |![done]   |                                       |
|listDatabases     |![done]   |                                       |
|listFunctions     |![done]   |                                       |
|listTables        |![done]   |                                       |
|recoverPartitions |![done]   |                                       |
|refreshByPath     |![done]   |                                       |
|refreshTable      |![done]   |                                       |
|registerFunction  |![open]   |                                       |
|setCurrentCatalog |![done]   |                                       |
|setCurrentDatabase|![done]   |                                       |
|tableExists       |![done]   |                                       |
|uncacheTable      |![done]   |                                       |


### DataFrameReader

|DataFrameReader  |API       |Comment                                |
|------------------|----------|---------------------------------------|
|csv               |![done]   |                                       |
|format            |![done]   |                                       |
|json              |![done]   |                                       |
|load              |![done]   |                                       |
|option            |![done]   |                                       |
|options           |![done]   |                                       |
|orc               |![done]   |                                       |
|parquet           |![done]   |                                       |
|schema            |![done]   |                                       |
|table             |![done]   |                                       |
|text              |![done]   |                                       |


### DataFrameWriter

Spark Connect *should* respect the format as long as your cluster supports the specified type and has the
required jars

|DataFrameWriter   |API       |Comment                                |
|------------------|----------|---------------------------------------|
|bucketBy          |![done]   |                                       |
|csv               |![done]   |                                       |
|format            |![done]   |                                       |
|insertInto        |![done]   |                                       |
|jdbc              |![open]   |                                       |
|json              |![done]   |                                       |
|mode              |![done]   |                                       |
|option            |![done]   |                                       |
|options           |![done]   |                                       |
|orc               |![done]   |                                       |
|parquet           |![done]   |                                       |
|partitionBy       |![done]   |                                       |
|save              |![done]   |                                       |
|saveAsTable       |![done]   |                                       |
|sortBy            |![done]   |                                       |
|text              |![done]   |                                       |


### DataFrameWriterV2

|DataFrameWriterV2 |API       |Comment                                |
|------------------|----------|---------------------------------------|
|append            |![done]   |                                       |
|create            |![done]   |                                       |
|createOrReplace   |![done]   |                                       |
|option            |![done]   |                                       |
|options           |![done]   |                                       |
|overwrite         |![done]   |                                       |
|overwritePartitions|![done]   |                                       |
|partitionedBy     |![done]   |                                       |
|replace           |![done]   |                                       |
|tableProperty     |![done]   |                                       |
|using             |![done]   |                                       |


### DataStreamReader

|DataStreamReader  |API       |Comment                                |
|------------------|----------|---------------------------------------|
|csv               |![open]   |                                       |
|format            |![done]   |                                       |
|json              |![open]   |                                       |
|load              |![done]   |                                       |
|option            |![done]   |                                       |
|options           |![done]   |                                       |
|orc               |![open]   |                                       |
|parquet           |![open]   |                                       |
|schema            |![done]   |                                       |
|table             |![open]   |                                       |
|text              |![open]   |                                       |


### DataStreamWriter

Start a streaming job and return a `StreamingQuery` object to handle the stream operations.

|DataStreamWriter  |API       |Comment                                |
|------------------|----------|---------------------------------------|
|foreach           |          |                                       |
|foreachBatch      |          |                                       |
|format            |![done]   |                                       |
|option            |![done]   |                                       |
|options           |![done]   |                                       |
|outputMode        |![done]   |Uses an Enum for `OutputMode`          |
|partitionBy       |![done]   |                                       |
|queryName         |![done]   |                                       |
|start             |![done]   |                                       |
|toTable           |![done]   |                                       |
|trigger           |![done]   |Uses an Enum for `TriggerMode`         |


### StreamingQuery

|StreamingQuery    |API       |Comment                               |
|------------------|----------|---------------------------------------|
|awaitTermination  |![done]   |                                       |
|exception         |![done]   |                                       |
|explain           |![done]   |                                       |
|processAllAvailable|![done]   |                                       |
|stop              |![done]   |                                       |
|id                |![done]   |                                       |
|isActive          |![done]   |                                       |
|lastProgress      |![done]   |                                       |
|name              |![done]   |                                       |
|recentProgress    |![done]   |                                       |
|runId             |![done]   |                                       |
|status            |![done]   |                                       |


### StreamingQueryManager

|StreamingQueryManager|API       |Comment                                |
|---------------------|----------|---------------------------------------|
|awaitAnyTermination  |![done]   |                                       |
|get                  |![done]   |                                       |
|resetTerminated      |![done]   |                                       |
|active               |![done]   |                                       |


### StreamingQueryListener

|StreamingQueryListener|API       |Comment                                |
|----------------------|----------|---------------------------------------|
|onQueryIdle           |![open]   |                                       |
|onQueryProgress       |![open]   |                                       |
|onQueryStarted        |![open]   |                                       |
|onQueryTerminated     |![open]   |                                       |


### DataFrame

Spark [DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) type object and its implemented traits.

| DataFrame                     | API     | Comment                                                    |
|-------------------------------|---------|------------------------------------------------------------|
| agg                           | ![done] |                                                            |
| alias                         | ![done] |                                                            |
| approxQuantile                | ![done] |                                                            |
| cache                         | ![done] |                                                            |
| checkpoint                    | ![open] | Not part of Spark Connect                                  |
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
| dropDuplicatesWithinWatermark | ![done] |                                                            |
| drop_duplicates               | ![done] |                                                            |
| dropna                        | ![done] |                                                            |
| dtypes                        | ![done] |                                                            |
| exceptAll                     | ![done] |                                                            |
| explain                       | ![done] |                                                            |
| fillna                        | ![done] |                                                            |
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
| isLocal                       | ![done] |                                                            |
| isStreaming                   | ![done] |                                                            |
| join                          | ![done] |                                                            |
| limit                         | ![done] |                                                            |
| localCheckpoint               | ![open] | Not part of Spark Connect                                  |
| mapInPandas                   | ![open] | TBD on this exact implementation                           |
| mapInArrow                    | ![open] | TBD on this exact implementation                           |
| melt                          | ![done] |                                                            |
| na                            | ![done] |                                                            |
| observe                       | ![open] |                                                            |
| offset                        | ![done] |                                                            |
| orderBy                       | ![done] |                                                            |
| persist                       | ![done] |                                                            |
| printSchema                   | ![done] |                                                            |
| randomSplit                   | ![done] |                                                            |
| registerTempTable             | ![done] |                                                            |
| repartition                   | ![done] |                                                            |
| repartitionByRange            | ![done] |                                                            |
| replace                       | ![done] |                                                            |
| rollup                        | ![done] |                                                            |
| sameSemantics                 | ![done] |                                                            |
| sample                        | ![done] |                                                            |
| sampleBy                      | ![done] |                                                            |
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
| summary                       | ![done] |                                                            |
| tail                          | ![done] |                                                            |
| take                          | ![done] |                                                            |
| to                            | ![done] |                                                            |
| toDF                          | ![done] |                                                            |
| toJSON                        | ![partial] | Does not return an `RDD` but a long JSON formatted `String` |
| toLocalIterator               | ![open] |                                                            |
| ~~toPandas~~ to_polars & toPolars  | ![partial] | Convert to a `polars::frame::DataFrame`            |
| **new** to_datafusion & toDataFusion | ![done] | Convert to a `datafusion::dataframe::DataFrame`     |
| transform                     | ![done] |                                                            |
| union                         | ![done] |                                                            |
| unionAll                      | ![done] |                                                            |
| unionByName                   | ![done] |                                                            |
| unpersist                     | ![done] |                                                            |
| unpivot                       | ![done] |                                                            |
| where                         | ![done] | use `filter` instead, `where` is a keyword for rust        |
| withColumn                    | ![done] |                                                            |
| withColumns                   | ![done] |                                                            |
| withColumnRenamed             | ![done] |                                                            |
| withColumnsRenamed            | ![done] |                                                            |
| withMetadata                  | ![done] |                                                            |
| withWatermark                 | ![done] |                                                            |
| write                         | ![done] |                                                            |
| writeStream                   | ![done] |                                                            |
| writeTo                       | ![done] |                                                            |


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
| dropFields       | ![done] |                                                                              |
| endswith         | ![done] |                                                                              |
| eqNullSafe       | ![open] |                                                                              |
| getField         | ![open] | This is depreciated but will need to be implemented                          |
| getItem          | ![open] | This is depreciated but will need to be implemented                          |
| ilike            | ![done] |                                                                              |
| isNotNull        | ![done] |                                                                              |
| isNull           | ![done] |                                                                              |
| isin             | ![done] |                                                                              |
| like             | ![done] |                                                                              |
| name             | ![done] |                                                                              |
| otherwise        | ![open] |                                                                              |
| over             | ![done] | Refer to **Window** for creating window specifications                       |
| rlike            | ![done] |                                                                              |
| startswith       | ![done] |                                                                              |
| substr           | ![done] |                                                                              |
| when             | ![open] |                                                                              |
| withField        | ![done] |                                                                              |
| eq `==`          | ![done] | Rust does not like when you try to overload `==` and return something other than a `bool`. Currently implemented column equality like `col('name').eq(col('id'))`. Not the best, but it works for now                                                                           |
| addition `+`     | ![done] |                                                                              |
| subtration `-`   | ![done] |                                                                              |
| multiplication `*` | ![done] |                                                                            |
| division `/`     | ![done] |                                                                              |
| OR  `\|`         | ![done] |                                                                              |
| AND `&`          | ![done] |                                                                              |
| XOR `^`          | ![done] |                                                                              |
| Negate `~`       | ![done] |                                                                              |


### Data Types

Data types are used for creating schemas and for casting columns to specific types

| Column                | API     | Comment           |
|-----------------------|---------|-------------------|
| ArrayType             | ![done] |                   |
| BinaryType            | ![done] |                   |
| BooleanType           | ![done] |                   |
| ByteType              | ![done] |                   |
| DateType              | ![done] |                   |
| DecimalType           | ![done] |                   |
| DoubleType            | ![done] |                   |
| FloatType             | ![done] |                   |
| IntegerType           | ![done] |                   |
| LongType              | ![done] |                   |
| MapType               | ![done] |                   |
| NullType              | ![done] |                   |
| ShortType             | ![done] |                   |
| StringType            | ![done] |                   |
| CharType              | ![done] |                   |
| VarcharType           | ![done] |                   |
| StructField           | ![done] |                   |
| StructType            | ![done] |                   |
| TimestampType         | ![done] |                   |
| TimestampNTZType      | ![done] |                   |
| DayTimeIntervalType   | ![done] |                   |
| YearMonthIntervalType | ![done] |                   |


### Literal Types

Create Spark literal types from these rust types. E.g. `lit(1_i64)` would be a `LongType()` in the schema.

An array can be made like `lit([1_i16,2_i16,3_i16])` would result in an `ArrayType(Short)` since all the values of the slice can be translated into literal type.

| Spark Literal Type | Rust Type           | Status  |
|--------------------|---------------------|---------|
| Null               |                     | ![open] |
| Binary             | `&[u8]`             | ![done] |
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
| Timestamp          | `chrono::DateTime<Tz>`  | ![done] |
| TimestampNtz       | `chrono::NaiveDateTime` | ![done] |
| CalendarInterval   |                     | ![open] |
| YearMonthInterval  |                     | ![open] |
| DayTimeInterval    |                     | ![open] |
| Array              | `slice` / `Vec`     | ![done] |
| Map                | Create with the function `create_map` | ![done] |
| Struct             | Create with the function `struct_col` or `named_struct` | ![done] |


### Window & WindowSpec

For ease of use it's recommended to use `Window` to create the `WindowSpec`.

| Window                  | API     | Comment |
|-------------------------|---------|---------|
| currentRow              | ![done] |         |
| orderBy                 | ![done] |         |
| partitionBy             | ![done] |         |
| rangeBetween            | ![done] |         |
| rowsBetween             | ![done] |         |
| unboundedFollowing      | ![done] |         |
| unboundedPreceding      | ![done] |         |
| WindowSpec.orderBy      | ![done] |         |
| WindowSpec.partitionBy  | ![done] |         |
| WindowSpec.rangeBetween | ![done] |         |
| WindowSpec.rowsBetween  | ![done] |         |


### Functions

Only a few of the functions are covered by unit tests. Functions involving closures or lambdas are not feasible.

| Functions                   | API     | Comments |
|-----------------------------|---------|----------|
| abs                         | ![done] |          |
| acos                        | ![done] |          |
| acosh                       | ![done] |          |
| add_months                  | ![done] |          |
| aes_decrypt                 | ![done] |          |
| aes_encrypt                 | ![done] |          |
| aggregate                   | ![open] |          |
| any_value                   | ![done] |          |
| approx_count_distinct       | ![done] |          |
| approx_percentile           | ![open] |          |
| array                       | ![done] |          |
| array_agg                   | ![done] |          |
| array_append                | ![done] |          |
| array_compact               | ![done] |          |
| array_contains              | ![done] |          |
| array_distinct              | ![done] |          |
| array_except                | ![done] |          |
| array_insert                | ![done] |          |
| array_intersect             | ![done] |          |
| array_join                  | ![done] |          |
| array_max                   | ![done] |          |
| array_min                   | ![done] |          |
| array_position              | ![done] |          |
| array_prepend               | ![done] |          |
| array_remove                | ![done] |          |
| array_repeat                | ![done] |          |
| array_size                  | ![done] |          |
| array_sort                  | ![open] |          |
| array_union                 | ![done] |          |
| arrays_overlap              | ![done] |          |
| arrays_zip                  | ![done] |          |
| asc                         | ![done] |          |
| asc_nulls_first             | ![done] |          |
| asc_nulls_last              | ![done] |          |
| ascii                       | ![done] |          |
| asin                        | ![done] |          |
| asinh                       | ![done] |          |
| assert_true                 | ![open] |          |
| atan                        | ![done] |          |
| atan2                       | ![done] |          |
| atanh                       | ![done] |          |
| avg                         | ![done] |          |
| base64                      | ![done] |          |
| bin                         | ![done] |          |
| bit_and                     | ![done] |          |
| bit_count                   | ![done] |          |
| bit_get                     | ![done] |          |
| bit_length                  | ![done] |          |
| bit_or                      | ![done] |          |
| bit_xor                     | ![done] |          |
| bitmap_bit_position         | ![done] |          |
| bitmap_bucket_number        | ![done] |          |
| bitmap_construct_agg        | ![done] |          |
| bitmap_count                | ![done] |          |
| bitmap_or_agg               | ![done] |          |
| bitwise_not                 | ![done] |          |
| bool_and                    | ![done] |          |
| bool_or                     | ![done] |          |
| broadcast                   | ![done] |          |
| bround                      | ![done] |          |
| btrim                       | ![open] |          |
| bucket                      | ![done] |          |
| call_function               | ![open] |          |
| call_udf                    | ![open] |          |
| cardinality                 | ![done] |          |
| cbrt                        | ![done] |          |
| ceil                        | ![done] |          |
| ceiling                     | ![done] |          |
| char                        | ![done] |          |
| char_length                 | ![done] |          |
| character_length            | ![done] |          |
| coalesce                    | ![done] |          |
| col                         | ![done] |          |
| collect_list                | ![done] |          |
| collect_set                 | ![done] |          |
| column                      | ![done] |          |
| concat                      | ![done] |          |
| concat_ws                   | ![open] |          |
| contains                    | ![done] |          |
| conv                        | ![done] |          |
| convert_timezone            | ![done] |          |
| corr                        | ![done] |          |
| cos                         | ![done] |          |
| cosh                        | ![done] |          |
| cot                         | ![done] |          |
| count                       | ![open] |          |
| count_distinct              | ![open] |          |
| count_if                    | ![done] |          |
| count_min_sketch            | ![done] |          |
| covar_pop                   | ![done] |          |
| covar_samp                  | ![done] |          |
| crc32                       | ![done] |          |
| create_map                  | ![done] |          |
| csc                         | ![done] |          |
| cume_dist                   | ![done] |          |
| curdate                     | ![done] |          |
| current_catalog             | ![done] |          |
| current_database            | ![done] |          |
| current_date                | ![done] |          |
| current_schema              | ![done] |          |
| current_timestamp           | ![done] |          |
| current_timezone            | ![done] |          |
| current_user                | ![done] |          |
| date_add                    | ![done] |          |
| date_diff                   | ![done] |          |
| date_format                 | ![done] |          |
| date_from_unix_date         | ![done] |          |
| date_part                   | ![done] |          |
| date_sub                    | ![done] |          |
| date_trunc                  | ![done] |          |
| dateadd                     | ![done] |          |
| datediff                    | ![done] |          |
| datepart                    | ![open] |          |
| day                         | ![done] |          |
| dayofmonth                  | ![done] |          |
| dayofweek                   | ![done] |          |
| dayofyear                   | ![done] |          |
| days                        | ![done] |          |
| decode                      | ![done] |          |
| degrees                     | ![done] |          |
| dense_rank                  | ![done] |          |
| desc                        | ![done] |          |
| desc_nulls_first            | ![done] |          |
| desc_nulls_last             | ![done] |          |
| e                           | ![done] |          |
| element_at                  | ![done] |          |
| elt                         | ![done] |          |
| encode                      | ![done] |          |
| endswith                    | ![done] |          |
| equal_null                  | ![done] |          |
| every                       | ![done] |          |
| exists                      | ![open] |          |
| exp                         | ![done] |          |
| explode                     | ![done] |          |
| explode_outer               | ![done] |          |
| expm1                       | ![done] |          |
| expr                        | ![done] |          |
| extract                     | ![done] |          |
| factorial                   | ![done] |          |
| filter                      | ![open] |          |
| find_in_set                 | ![done] |          |
| first                       | ![done] |          |
| first_value                 | ![done] |          |
| flatten                     | ![done] |          |
| floor                       | ![done] |          |
| forall                      | ![open] |          |
| format_number               | ![done] |          |
| format_string               | ![open] |          |
| from_csv                    | ![open] |          |
| from_json                   | ![open] |          |
| from_unixtime               | ![done] |          |
| from_utc_timestamp          | ![done] |          |
| get                         | ![done] |          |
| get_json_object             | ![done] |          |
| getbit                      | ![done] |          |
| greatest                    | ![done] |          |
| grouping                    | ![done] |          |
| grouping_id                 | ![done] |          |
| hash                        | ![done] |          |
| hex                         | ![done] |          |
| histogram_numeric           | ![done] |          |
| hll_sketch_agg              | ![done] |          |
| hll_sketch_estimate         | ![done] |          |
| hll_union                   | ![open] |          |
| hll_union_agg               | ![done] |          |
| hour                        | ![done] |          |
| hours                       | ![done] |          |
| hypot                       | ![done] |          |
| ifnull                      | ![done] |          |
| ilike                       | ![open] |          |
| initcap                     | ![done] |          |
| inline                      | ![done] |          |
| inline_outer                | ![done] |          |
| input_file_block_length     | ![done] |          |
| input_file_block_start      | ![done] |          |
| input_file_name             | ![done] |          |
| instr                       | ![done] |          |
| isnan                       | ![done] |          |
| isnotnull                   | ![done] |          |
| isnull                      | ![done] |          |
| java_method                 | ![done] |          |
| json_array_length           | ![done] |          |
| json_object_keys            | ![done] |          |
| json_tuple                  | ![done] |          |
| kurtosis                    | ![done] |          |
| lag                         | ![open] |          |
| last                        | ![done] |          |
| last_day                    | ![done] |          |
| last_value                  | ![done] |          |
| lcase                       | ![done] |          |
| lead                        | ![open] |          |
| least                       | ![done] |          |
| left                        | ![done] |          |
| length                      | ![done] |          |
| levenshtein                 | ![open] |          |
| like                        | ![open] |          |
| lit                         | ![done] |          |
| ln                          | ![done] |          |
| localtimestamp              | ![done] |          |
| locate                      | ![open] |          |
| log                         | ![done] |          |
| log10                       | ![done] |          |
| log1p                       | ![done] |          |
| log2                        | ![done] |          |
| lower                       | ![done] |          |
| lpad                        | ![done] |          |
| ltrim                       | ![done] |          |
| make_date                   | ![done] |          |
| make_dt_interval            | ![done] |          |
| make_interval               | ![done] |          |
| make_timestamp              | ![done] |          |
| make_timestamp_ltz          | ![done] |          |
| make_timestamp_ntz          | ![done] |          |
| make_ym_interval            | ![done] |          |
| map_concat                  | ![done] |          |
| map_contains_key            | ![done] |          |
| map_entries                 | ![done] |          |
| map_filter                  | ![open] |          |
| map_from_arrays             | ![done] |          |
| map_from_entries            | ![done] |          |
| map_keys                    | ![done] |          |
| map_values                  | ![done] |          |
| map_zip_with                | ![open] |          |
| mask                        | ![open] |          |
| max                         | ![done] |          |
| max_by                      | ![done] |          |
| md5                         | ![done] |          |
| mean                        | ![done] |          |
| median                      | ![done] |          |
| min                         | ![done] |          |
| min_by                      | ![done] |          |
| minute                      | ![done] |          |
| mode                        | ![done] |          |
| monotonically_increasing_id | ![done] |          |
| month                       | ![done] |          |
| months                      | ![done] |          |
| months_between              | ![done] |          |
| named_struct                | ![done] |          |
| nanvl                       | ![done] |          |
| negate                      | ![done] |          |
| negative                    | ![done] |          |
| next_day                    | ![done] |          |
| now                         | ![done] |          |
| nth_value                   | ![open] |          |
| ntile                       | ![done] |          |
| nullif                      | ![done] |          |
| nvl                         | ![done] |          |
| nvl2                        | ![done] |          |
| octet_length                | ![done] |          |
| overlay                     | ![open] |          |
| pandas_udf                  | ![open] |          |
| parse_url                   | ![open] |          |
| percent_rank                | ![done] |          |
| percentile                  | ![done] |          |
| percentile_approx           | ![done] |          |
| pi                          | ![done] |          |
| pmod                        | ![done] |          |
| posexplode                  | ![done] |          |
| posexplode_outer            | ![done] |          |
| position                    | ![open] |          |
| positive                    | ![open] |          |
| pow                         | ![done] |          |
| power                       | ![done] |          |
| printf                      | ![open] |          |
| product                     | ![done] |          |
| quarter                     | ![done] |          |
| radians                     | ![done] |          |
| raise_error                 | ![done] |          |
| rand                        | ![done] |          |
| randn                       | ![done] |          |
| rank                        | ![done] |          |
| reduce                      | ![open] |          |
| reflect                     | ![done] |          |
| regexp                      | ![done] |          |
| regexp_count                | ![done] |          |
| regexp_extract              | ![open] |          |
| regexp_extract_all          | ![open] |          |
| regexp_instr                | ![open] |          |
| regexp_like                 | ![done] |          |
| regexp_replace              | ![open] |          |
| regexp_substr               | ![done] |          |
| regr_avgx                   | ![done] |          |
| regr_avgy                   | ![done] |          |
| regr_count                  | ![done] |          |
| regr_intercept              | ![done] |          |
| regr_r2                     | ![done] |          |
| regr_slope                  | ![done] |          |
| regr_sxx                    | ![done] |          |
| regr_sxy                    | ![done] |          |
| regr_syy                    | ![done] |          |
| repeat                      | ![done] |          |
| replace                     | ![open] |          |
| reverse                     | ![done] |          |
| right                       | ![done] |          |
| rint                        | ![done] |          |
| rlike                       | ![done] |          |
| round                       | ![done] |          |
| row_number                  | ![done] |          |
| rpad                        | ![done] |          |
| rtrim                       | ![done] |          |
| schema_of_csv               | ![open] |          |
| schema_of_json              | ![open] |          |
| sec                         | ![done] |          |
| second                      | ![done] |          |
| sentences                   | ![open] |          |
| sequence                    | ![done] |          |
| session_window              | ![open] |          |
| sha                         | ![done] |          |
| sha1                        | ![done] |          |
| sha2                        | ![done] |          |
| shiftleft                   | ![done] |          |
| shiftright                  | ![done] |          |
| shiftrightunsigned          | ![open] |          |
| shuffle                     | ![done] |          |
| sign                        | ![done] |          |
| signum                      | ![done] |          |
| sin                         | ![done] |          |
| sinh                        | ![done] |          |
| size                        | ![done] |          |
| skewness                    | ![done] |          |
| slice                       | ![done] |          |
| some                        | ![done] |          |
| sort_array                  | ![done] |          |
| soundex                     | ![done] |          |
| spark_partition_id          | ![done] |          |
| split                       | ![open] |          |
| split_part                  | ![done] |          |
| sqrt                        | ![done] |          |
| stack                       | ![done] |          |
| startswith                  | ![done] |          |
| std                         | ![done] |          |
| stddev                      | ![done] |          |
| stddev_pop                  | ![done] |          |
| stddev_samp                 | ![done] |          |
| str_to_map                  | ![open] |          |
| struct                      | ![open] |          |
| substr                      | ![open] |          |
| substring                   | ![done] |          |
| substring_index             | ![done] |          |
| sum                         | ![done] |          |
| sum_distinct                | ![done] |          |
| tan                         | ![done] |          |
| tanh                        | ![done] |          |
| timestamp_micros            | ![done] |          |
| timestamp_millis            | ![done] |          |
| timestamp_seconds           | ![done] |          |
| to_binary                   | ![open] |          |
| to_char                     | ![done] |          |
| to_csv                      | ![open] |          |
| to_date                     | ![done] |          |
| to_json                     | ![open] |          |
| to_number                   | ![done] |          |
| to_timestamp                | ![done] |          |
| to_timestamp_ltz            | ![done] |          |
| to_timestamp_ntz            | ![done] |          |
| to_unix_timestamp           | ![done] |          |
| to_utc_timestamp            | ![done] |          |
| to_varchar                  | ![done] |          |
| to_degrees                  | ![done] |          |
| to_radians                  | ![done] |          |
| transform                   | ![open] |          |
| transform_keys              | ![open] |          |
| transform_values            | ![open] |          |
| translate                   | ![done] |          |
| trim                        | ![done] |          |
| trunc                       | ![done] |          |
| try_add                     | ![open] |          |
| try_aes_decrypt             | ![open] |          |
| try_avg                     | ![open] |          |
| try_divide                  | ![open] |          |
| try_element_at              | ![done] |          |
| try_multiply                | ![open] |          |
| try_subtract                | ![open] |          |
| try_sum                     | ![open] |          |
| try_to_binary               | ![open] |          |
| try_to_number               | ![open] |          |
| try_to_timestamp            | ![open] |          |
| typeof                      | ![open] |          |
| ucase                       | ![done] |          |
| udf                         | ![open] |          |
| udtf                        | ![open] |          |
| unbase64                    | ![done] |          |
| unhex                       | ![done] |          |
| unix_date                   | ![done] |          |
| unix_micros                 | ![open] |          |
| unix_millis                 | ![done] |          |
| unix_seconds                | ![done] |          |
| unix_timestamp              | ![done] |          |
| unwrap_udt                  | ![open] |          |
| upper                       | ![done] |          |
| url_decode                  | ![done] |          |
| url_encode                  | ![done] |          |
| user                        | ![done] |          |
| var_pop                     | ![done] |          |
| var_samp                    | ![done] |          |
| variance                    | ![done] |          |
| version                     | ![done] |          |
| weekday                     | ![done] |          |
| weekofyear                  | ![done] |          |
| when                        | ![open] |          |
| width_bucket                | ![done] |          |
| window                      | ![open] |          |
| window_time                 | ![done] |          |
| xpath                       | ![done] |          |
| xpath_boolean               | ![done] |          |
| xpath_double                | ![done] |          |
| xpath_float                 | ![done] |          |
| xpath_int                   | ![done] |          |
| xpath_long                  | ![done] |          |
| xpath_number                | ![done] |          |
| xpath_short                 | ![done] |          |
| xpath_string                | ![done] |          |
| xxhash64                    | ![done] |          |
| year                        | ![done] |          |
| years                       | ![done] |          |
| zip_with                    | ![open] |          |


### UdfRegistration (may not be possible)

|UDFRegistration   |API       |Comment                                |
|------------------|----------|---------------------------------------|
|register          |![open]   |                                       |
|registerJavaFunction|![open]   |                                       |
|registerJavaUDAF  |![open]   |                                       |

### UdtfRegistration (may not be possible)

|UDTFRegistration  |API       |Comment                                |
|------------------|----------|---------------------------------------|
|register          |![open]   |                                       |


[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
[partial]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueDrafted.svg
