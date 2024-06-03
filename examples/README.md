# Examples

Set of examples that show off different features provided by `spark-connect-rs` client.

In order to build these examples, you must have the `protoc` protocol buffers compilter
installed, along with the git submodule synced.

```bash
git clone https://github.com/sjrusso8/spark-connect-rs.git
git submodule update --init --recursive
```

### sql

Write a simple SQL statement and save the dataframe as a parquet

```bash
$ cargo run --bin sql
```

### reader

Read a CSV file, select specific columns, and display the results

```bash
$ cargo run --bin reader
```

### writer

Create a dataframe, and save the results to a file

```bash
$ cargo run --bin writer
```

### readstream

Create a streaming query, and monitor the progress of the stream

```bash
$ cargo run --bin readstream
```

### deltalake

Read a file into a dataframe, save the result as a deltalake table, and append a new record to the table.

**Prerequisite** the spark cluster must be started with the deltalake package. The `docker-compose.yml` provided in the repo has deltalake pre-installed.
Or if you are running a spark connect server location, run the below scripts first

If you are running a local spark connect server. The Delta Lake jars need to be added onto the server before it starts.

```bash
$ $SPARK_HOME/sbin/start-connect-server.sh --packages "org.apache.spark:spark-connect_2.12:3.5.1,io.delta:delta-spark_2.12:3.0.0" \
      --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
      --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

```bash
$ cargo run --bin deltalake
```

### databricks

Read a Unity Catalog table, perform an aggregation, and display the results.

**Prerequisite** must have access to a Databricks workspace, a personal access token, and cluster running >=13.3LTS.

```bash
$ cargo run --bin databricks --features=tls
```
