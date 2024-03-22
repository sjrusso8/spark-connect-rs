//! A re-implementation of Spark functions

use crate::expressions;
use crate::spark;
use crate::DataFrame;

use crate::column::Column;
use expressions::{ToExpr, ToLiteralExpr};

use crate::generate_functions;
use crate::utils::invoke_func;

use rand::random;

/// Create a column from a &str
pub fn col(value: &str) -> Column {
    Column::from(value)
}

/// Create a column from a &str
pub fn column(value: &str) -> Column {
    Column::from(value)
}

/// Create a literal value from a rust data type
pub fn lit<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr())
}

#[allow(dead_code)]
#[allow(unused_variables)]
fn broadcast(df: DataFrame) {
    unimplemented!("not implemented")
}

pub fn rand(seed: Option<i32>) -> Column {
    invoke_func("rand", vec![lit(seed.unwrap_or(random::<i32>()))])
}

pub fn randn(seed: Option<i32>) -> Column {
    invoke_func("randn", vec![lit(seed.unwrap_or(random::<i32>()))])
}

#[allow(dead_code)]
#[allow(unused_variables)]
fn when<T: ToLiteralExpr>(condition: Column, value: T) -> Column {
    unimplemented!("not implemented")
}

pub fn bitwise_not<T: ToExpr>(col: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("~", vec![col])
}

pub fn expr(val: &str) -> Column {
    Column::from(spark::Expression {
        expr_type: Some(spark::expression::ExprType::ExpressionString(
            spark::expression::ExpressionString {
                expression: val.to_string(),
            },
        )),
    })
}

pub fn log<T: ToExpr>(arg1: T, arg2: Option<T>) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    match arg2 {
        Some(arg2) => invoke_func("log", vec![arg1, arg2]),
        None => ln(arg1),
    }
}

pub fn pow<T: ToExpr>(col1: T, col2: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    power(col1, col2)
}

pub fn round<T: ToExpr + ToLiteralExpr>(col: T, scale: Option<f32>) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    let values = vec![lit(col), lit(scale.unwrap_or(0.0)).clone()];
    invoke_func("round", values)
}

pub fn add_months<T: ToExpr>(start: T, months: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("add_months", vec![start, months])
}

pub fn date_add<T: ToExpr>(start: T, days: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("date_add", vec![start, days])
}

pub fn dateadd<T: ToExpr>(start: T, days: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("dateadd", vec![start, days])
}

pub fn datediff<T: ToExpr>(end: T, start: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("datediff", vec![end, start])
}

pub fn date_sub<T: ToExpr>(start: T, end: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("date_sub", vec![start, end])
}

pub fn character_length<T: ToExpr>(str: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("character_length", vec![str])
}

pub fn char_length<T: ToExpr>(str: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("char_length", vec![str])
}

pub fn ucase<T: ToExpr>(str: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("ucase", vec![str])
}

pub fn asc<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr()).asc()
}

pub fn asc_nulls_first<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr()).asc_nulls_first()
}

pub fn asc_nulls_last<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr()).asc_nulls_last()
}

pub fn desc<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr()).desc()
}

pub fn desc_nulls_first<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr()).desc_nulls_first()
}

pub fn desc_nulls_last<T: ToLiteralExpr>(col: T) -> Column {
    Column::from(col.to_literal_expr()).desc_nulls_last()
}

// functions that require no arguments
generate_functions!(
    no_args: pi, input_file_name,
    monotonically_increasing_id,
    spark_partition_id,
    e,
    curdate,
    current_date,
    current_timezone,
    now,
    version,
    user,
    input_file_block_start,
    input_file_block_length,
    current_user,
    current_schema,
    current_database,
    current_catalog,
    row_number,
    rank,
    percent_rank,
    dense_rank,
    cume_dist
);

// functions that require a single col argument
generate_functions!(
    one_col: isnan,
    isnull,
    sqrt,
    abs,
    bin,
    ceil,
    ceiling,
    exp,
    factorial,
    floor,
    ln,
    log10,
    log1p,
    log2,
    negate,
    negative,
    day,
    dayofmonth,
    dayofweek,
    dayofyear,
    second,
    minute,
    hour,
    weekday,
    weekofyear,
    year,
    quarter,
    month,
    timestamp_micros,
    timestamp_millis,
    timestamp_seconds,
    unix_date,
    unix_millis,
    unix_macros,
    unix_seconds,
    ascii,
    base64,
    bit_length,
    char,
    length,
    lower,
    ltrim,
    unbase64,
    upper,
    trim,
    crc32,
    sha1,
    md5,
    sha,
    bitmap_or_agg,
    bitmap_count,
    bitmap_construct_agg,
    bitmap_bucket_number,
    bitmap_bit_position,
    bit_count,
    soundex,
    rtrim,
    octet_length,
    initcap,
    years,
    months,
    days,
    hours,
    map_from_entries,
    map_entries,
    map_values,
    map_keys,
    flatten,
    reverse,
    shuffle,
    array_min,
    array_max,
    cardinality,
    size,
    json_object_keys,
    json_array_length,
    inline_outer,
    inline,
    posexplode_outer,
    posexplode,
    explode_outer,
    explode,
    array_compact,
    array_distinct,
    array_size
);

// functions that require exactly two col arguments
generate_functions!(
    two_cols: nvl,
    nullif,
    isnotnull,
    ifnull,
    equal_null,
    array_except,
    array_union,
    array_intersect,
    nanvl,
    power
);

// functions that require one or more col arguments
generate_functions!(
    multiple_cols: coalesce,
    named_struct,
    least,
    greatest,
    stack,
    java_method,
    reflect,
    xxhash64,
    hash,
    map_concat,
    arrays_zip,
    concat,
    create_map,
    array
);

#[cfg(test)]
mod tests {

    // use arrow::{
    //     array::Int64Array,
    //     datatypes::{DataType, Field, Schema},
    //     record_batch::RecordBatch,
    // };

    // TODO Update the tests to validate against an arrow dataframe
    use super::*;

    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::{SparkSession, SparkSessionBuilder};

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_func";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_func_lit() {
        let spark = setup().await;

        let mut df = spark
            .range(None, 1, 1, Some(1))
            .select(vec![lit(5).alias("height"), col("id")]);

        let row = df.collect().await.unwrap();

        let schema = Schema::new(vec![
            Field::new("height", DataType::Int32, false),
            Field::new("id", DataType::Int64, false),
        ]);

        let height_val = Int32Array::from(vec![5]);
        let id_val = Int64Array::from(vec![0]);

        let expected_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(height_val), Arc::new(id_val)],
        )
        .unwrap();

        assert_eq!(expected_batch, row[0]);
    }

    #[tokio::test]
    async fn test_func_asc() {
        let spark = setup().await;

        let mut df_col_asc = spark
            .clone()
            .range(Some(1), 3, 1, Some(1))
            .sort(vec![col("id").asc()]);

        let mut df_func_asc = spark
            .range(Some(1), 3, 1, Some(1))
            .sort(vec![asc(col("id"))]);

        let rows_col_asc = df_col_asc.collect().await.unwrap();
        let rows_func_asc = df_func_asc.collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let id_val = Int64Array::from(vec![1, 2]);

        let expected_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_val)]).unwrap();

        assert_eq!(expected_batch.clone(), rows_col_asc[0]);
        assert_eq!(expected_batch, rows_func_asc[0]);
    }

    #[tokio::test]
    async fn test_func_desc() {
        let spark = setup().await;

        let mut df_col_asc = spark
            .clone()
            .range(Some(1), 3, 1, Some(1))
            .sort(vec![col("id").desc()]);

        let mut df_func_asc = spark
            .range(Some(1), 3, 1, Some(1))
            .sort(vec![desc(col("id"))]);

        let rows_col_asc = df_col_asc.collect().await.unwrap();
        let rows_func_asc = df_func_asc.collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let id_val = Int64Array::from(vec![2, 1]);

        let expected_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_val)]).unwrap();

        assert_eq!(expected_batch.clone(), rows_col_asc[0]);
        assert_eq!(expected_batch, rows_func_asc[0]);
    }

    #[tokio::test]
    async fn test_func_sqrt() {
        let spark = setup().await;

        let mut df = spark.range(None, 1, 1, Some(1)).select(sqrt(lit(4)));

        let row = df.collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("SQRT(4)", DataType::Float64, true)]);

        let val = Float64Array::from(vec![2.0]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)]).unwrap();

        assert_eq!(expected_batch, row[0]);
    }

    #[tokio::test]
    async fn test_func_add() {
        let spark = setup().await;

        let mut df = spark
            .range(Some(1), 3, 1, Some(1))
            .select((lit(4) + col("id")).alias("add"));

        let row = df.collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("add", DataType::Int64, false)]);

        let val = Int64Array::from(vec![5, 6]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)]).unwrap();

        assert_eq!(expected_batch, row[0]);
    }

    #[tokio::test]
    async fn test_func_substract() {
        let spark = setup().await;

        let mut df = spark
            .range(Some(1), 3, 1, Some(1))
            .select((lit(4) - col("id")).alias("add"));

        let row = df.collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("add", DataType::Int64, false)]);

        let val = Int64Array::from(vec![3, 2]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)]).unwrap();

        assert_eq!(expected_batch, row[0]);
    }

    #[tokio::test]
    async fn test_func_multiple() {
        let spark = setup().await;

        let mut df = spark
            .range(Some(1), 3, 1, Some(1))
            .select((lit(4) * col("id")).alias("add"));

        let row = df.collect().await.unwrap();

        let schema = Schema::new(vec![Field::new("add", DataType::Int64, false)]);

        let val = Int64Array::from(vec![4, 8]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)]).unwrap();

        assert_eq!(expected_batch, row[0]);
    }

    #[tokio::test]
    async fn test_func_col_contains() {
        let spark = setup().await;

        let path = ["/opt/spark/examples/src/main/resources/people.csv"];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path);

        let row = df
            .filter(col("name").contains("e"))
            .select("name")
            .collect()
            .await
            .unwrap();

        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let val = StringArray::from(vec!["Jorge"]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)]).unwrap();

        assert_eq!(expected_batch, row[0]);
    }

    #[tokio::test]
    async fn test_func_col_isin() {
        let spark = setup().await;

        let path = ["/opt/spark/examples/src/main/resources/people.csv"];

        let mut df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path);

        let row = df
            .filter(col("name").isin(vec!["Jorge", "Bob"]))
            .select("name")
            .collect()
            .await
            .unwrap();

        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let val = StringArray::from(vec!["Jorge", "Bob"]);

        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)]).unwrap();

        assert_eq!(expected_batch, row[0]);
    }
}
