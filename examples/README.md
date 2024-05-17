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

Reader a CSV file, select specific columns, and display the results

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

```bash
$ cargo run --bin deltalake
```

### databricks

Read a Unity Catalog table, perform an aggregation, and display the results.

**Prerequisite** must have access to a Databricks workspace, a personal access token, and cluster running >=13.3LTS.

```bash
$ cargo run --bin databricks --features=tls
```
