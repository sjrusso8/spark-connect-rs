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
├── core       <- core implementation in Rust
│   └─ spark   <- git submodule for apache/spark
├── rust       <- shim for 'spark-connect-rs' from core
├── examples   <- examples of using different aspects of the crate
├── datasets   <- sample files from the main spark repo
```

Future state would be to have additional bindings for other languages along side the top level `rust` folder.

## Getting Started

This section explains how run Spark Connect Rust locally starting from 0.

**Step 1**: Install rust via rustup: https://www.rust-lang.org/tools/install

**Step 2**: Ensure you have a [cmake](https://cmake.org/download/) and [protobuf](https://grpc.io/docs/protoc-installation/) installed on your machine

**Step 3**: Run the following commands to clone the repo

```bash
git clone https://github.com/sjrusso8/spark-connect-rs.git
git submodule update --init --recursive

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
- ![open] StreamingQueryManager
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
|interruptAll      |![done]   |  splitn                                     |
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
|streams           |![open]   |[Streams](#streamingquerymanager)      |
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

### StreamingQueryManager

|StreamingQueryManager|API       |Comment                                |
|---------------------|----------|---------------------------------------|
|awaitAnyTermination  |![open]   |                                       |
|get                  |![open]   |                                       |
|resetTerminated      |![open]   |                                       |
|active               |![open]   |                                       |

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

### DataFrameReader

|DataFrameReader  |API       |Comment                                |
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
|table             |![done]   |                                       |
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

### StreamingQueryListener

|StreamingQueryListener|API       |Comment                                |
|----------------------|----------|---------------------------------------|
|onQueryIdle           |![open]   |                                       |
|onQueryProgress       |![open]   |                                       |
|onQueryStarted        |![open]   |                                       |
|onQueryTerminated     |![open]   |                                       |

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
|createExternalTale|![open]   |                                       |
|createTable       |![open]   |                                       |
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
| columns                       | ![done] |                                                           |
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
| withColumnRenamed             | ![open] |                                                            |
| withColumnsRenamed            | ![done] |                                                            |
| withMetadata                  | ![open] |                                                            |
| withWatermark                 | ![open] |                                                            |
| write                         | ![done] |                                                            |
| writeStream                   | ![done] |                                                            |
| writeTo                       | ![done] |                                                            |

### DataFrameWriter

Spark Connect *should* respect the format as long as your cluster supports the specified type and has the
required jars

|DataFrameWriter   |API       |Comment                                |
|------------------|----------|---------------------------------------|
|bucketBy          |![done]   |                                       |
|csv               |          |                                       |
|format            |![done]   |                                       |
|insertInto        |![done]   |                                       |
|jdbc              |          |                                       |
|json              |          |                                       |
|mode              |![done]   |                                       |
|option            |![done]   |                                       |
|options           |![done]   |                                       |
|orc               |          |                                       |
|parquet           |          |                                       |
|partitionBy       |          |                                       |
|save              |![done]   |                                       |
|saveAsTable       |![done]   |                                       |
|sortBy            |![done]   |                                       |
|text              |          |                                       |

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


### Functions

Only a few of the functions are covered by unit tests.

| Functions                   | API     | Comment |
|-----------------------------|---------|---------|
| abs                         | ![done] |         |
| acos                        | ![done] |         |
| acosh                       | ![done] |         |
| add_months                  | ![done] |         |
| aggregate                   | ![open] |         |
| approxCountDistinct         | ![open] |         |
| approx_count_distinct       | ![done] |         |
| array                       | ![done] |         |
| array_append                | ![done] |         |
| array_compact               | ![done] |         |
| array_contains              | ![open] |         |
| array_distinct              | ![done] |         |
| array_except                | ![done] |         |
| array_insert                | ![open] |         |
| array_intersect             | ![done] |         |
| array_join                  | ![open] |         |
| array_max                   | ![done] |         |
| array_min                   | ![done] |         |
| array_position              | ![done] |         |
| array_remove                | ![done] |         |
| array_repeat                | ![done] |         |
| array_sort                  | ![open] |         |
| array_union                 | ![done] |         |
| arrays_overlap              | ![open] |         |
| arrays_zip                  | ![done] |         |
| asc                         | ![done] |         |
| asc_nulls_first             | ![done] |         |
| asc_nulls_last              | ![done] |         |
| ascii                       | ![done] |         |
| asin                        | ![done] |         |
| asinh                       | ![done] |         |
| assert_true                 | ![open] |         |
| atan                        | ![done] |         |
| atan2                       | ![done] |         |
| atanh                       | ![done] |         |
| avg                         | ![done] |         |
| base64                      | ![done] |         |
| bin                         | ![done] |         |
| bit_length                  | ![done] |         |
| bitwiseNOT                  | ![open] |         |
| bitwise_not                 | ![done] |         |
| broadcast                   | ![open] |         |
| bround                      | ![open] |         |
| bucket                      | ![open] |         |
| call_udf                    | ![open] |         |
| cbrt                        | ![done] |         |
| ceil                        | ![done] |         |
| coalesce                    | ![done] |         |
| col                         | ![done] |         |
| collect_list                | ![done] |         |
| collect_set                 | ![done] |         |
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
| covar_pop                   | ![done] |         |
| covar_samp                  | ![done] |         |
| crc32                       | ![done] |         |
| create_map                  | ![done] |         |
| csc                         | ![done] |         |
| cume_dist                   | ![done] |         |
| current_date                | ![done] |         |
| current_timestamp           | ![done] |         |
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
| degrees                     | ![done] |         |
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
| expm1                       | ![done] |         |
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
| grouping                    | ![done] |         |
| grouping_id                 | ![open] |         |
| has_numpy                   | ![open] |         |
| hash                        | ![done] |         |
| hex                         | ![done] |         |
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
| kurtosis                    | ![done] |         |
| lag                         | ![open] |         |
| last                        | ![open] |         |
| last_day                    | ![open] |         |
| lead                        | ![open] |         |
| least                       | ![done] |         |
| length                      | ![done] |         |
| levenshtein                 | ![open] |         |
| lit                         | ![done] |         |
| localtimestamp              | ![done] |         |
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
| max                         | ![done] |         |
| max_by                      | ![open] |         |
| md5                         | ![done] |         |
| mean                        | ![done] |         |
| median                      | ![done] |         |
| min                         | ![done] |         |
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
| ntile                       | ![done] |         |
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
| product                     | ![done] |         |
| quarter                     | ![done] |         |
| radians                     | ![done] |         |
| raise_error                 | ![open] |         |
| rand                        | ![done] |         |
| randn                       | ![done] |         |
| rank                        | ![done] |         |
| regexp_extract              | ![open] |         |
| regexp_replace              | ![open] |         |
| repeat                      | ![open] |         |
| reverse                     | ![done] |         |
| rint                        | ![done] |         |
| round                       | ![done] |         |
| row_number                  | ![done] |         |
| rpad                        | ![open] |         |
| rtrim                       | ![done] |         |
| schema_of_csv               | ![open] |         |
| schema_of_json              | ![open] |         |
| sec                         | ![done] |         |
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
| signum                      | ![done] |         |
| sin                         | ![done] |         |
| sinh                        | ![done] |         |
| size                        | ![done] |         |
| skewness                    | ![done] |         |
| slice                       | ![open] |         |
| sort_array                  | ![open] |         |
| soundex                     | ![done] |         |
| spark_partition_id          | ![done] |         |
| split                       | ![open] |         |
| sqrt                        | ![done] |         |
| stddev                      | ![done] |         |
| stddev_pop                  | ![done] |         |
| stddev_samp                 | ![done] |         |
| struct                      | ![open] |         |
| substring                   | ![open] |         |
| substring_index             | ![open] |         |
| sum                         | ![done] |         |
| sumDistinct                 | ![open] |         |
| sum_distinct                | ![open] |         |
| sys                         | ![open] |         |
| tan                         | ![done] |         |
| tanh                        | ![done] |         |
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
| unhex                       | ![done] |         |
| unix_timestamp              | ![open] |         |
| unwrap_udt                  | ![open] |         |
| upper                       | ![done] |         |
| var_pop                     | ![done] |         |
| var_samp                    | ![done] |         |
| variance                    | ![done] |         |
| warnings                    | ![open] |         |
| weekofyear                  | ![done] |         |
| when                        | ![open] |         |
| window                      | ![open] |         |
| window_time                 | ![open] |         |
| xxhash64                    | ![done] |         |
| year                        | ![done] |         |
| years                       | ![done] |         |
| zip_with                    | ![open] |         |


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


[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
[partial]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueDrafted.svg
