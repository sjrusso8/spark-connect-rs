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

pub fn approx_count_distinct<T: ToExpr + ToLiteralExpr>(col: T, rsd: Option<f32>) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    match rsd {
        Some(rsd) => invoke_func("approx_count_distinct", vec![lit(col), lit(rsd)]),
        None => invoke_func("approx_count_distinct", vec![col]),
    }
}

pub fn array_append<T: ToExpr + ToLiteralExpr>(col: T, value: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("array_append", vec![lit(col), lit(value)])
}

pub fn array_insert<T: ToExpr + ToLiteralExpr>(col: T, pos: T, value: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("array_insert", vec![lit(col), lit(pos), lit(value)])
}

pub fn array_join<T: ToExpr + ToLiteralExpr>(
    col: T,
    delimiter: &str,
    null_replacement: Option<&str>,
) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    match null_replacement {
        Some(replacement) => invoke_func(
            "array_join",
            vec![lit(col), lit(delimiter), lit(replacement)],
        ),
        None => invoke_func("array_join", vec![lit(col), lit(delimiter)]),
    }
}

pub fn array_position<T: ToExpr + ToLiteralExpr>(col: T, value: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("array_position", vec![lit(col), lit(value)])
}

pub fn array_remove<T: ToExpr + ToLiteralExpr>(col: T, element: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("array_remove", vec![lit(col), lit(element)])
}

pub fn array_repeat<T: ToExpr + ToLiteralExpr>(col: T, count: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("array_repeat", vec![lit(col), lit(count)])
}

pub fn array_overlap<T: ToExpr + ToLiteralExpr>(a1: T, a2: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("array_overlap", vec![lit(a1), lit(a2)])
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

pub fn mean<T: ToExpr>(col: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    avg(col)
}

pub fn ntile(n: i32) -> Column {
    invoke_func("ntitle", lit(n))
}

pub fn struct_col<T: expressions::ToVecExpr>(cols: T) -> Column {
    invoke_func("struct", cols)
}

pub fn negate<T: expressions::ToExpr>(col: T) -> Column
where
    Vec<T>: expressions::ToVecExpr,
{
    invoke_func("not", vec![col])
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
    cume_dist,
    current_timestamp,
    localtimestamp
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
    array_size,
    acos,
    acosh,
    asin,
    asinh,
    atan,
    atanh,
    avg,
    cbrt,
    collect_set,
    collect_list,
    csc,
    degrees,
    expm1,
    grouping,
    hex,
    kurtosis,
    max,
    median,
    min,
    product,
    radians,
    rint,
    sec,
    signum,
    sin,
    sinh,
    skewness,
    stddev,
    stddev_pop,
    stddev_samp,
    sum,
    tan,
    tanh,
    unhex,
    var_pop,
    var_samp,
    variance
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
    power,
    atan2,
    covar_pop,
    covar_samp
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
    array,
    group_id
);

#[cfg(test)]
mod tests {

    use super::*;

    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::errors::SparkError;
    use crate::{SparkSession, SparkSessionBuilder};

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_func;session_id=78de1054-ff56-4665-a3a2-e337c6ca525e";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_func_lit() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .clone()
            .range(None, 1, 1, Some(1))
            .select([lit(5).alias("height"), col("id")]);

        let row = df.collect().await?;

        let height: ArrayRef = Arc::new(Int32Array::from(vec![5]));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![0]));

        let expected = RecordBatch::try_from_iter(vec![("height", height), ("id", id)])?;

        assert_eq!(expected.clone(), row);

        let df = spark
            .clone()
            .range(None, 1, 1, Some(1))
            .select(lit([1, 2, 3]));

        let row = df.collect().await?;

        assert_eq!(1, row.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn test_func_asc() -> Result<(), SparkError> {
        let spark = setup().await;

        let df_col_asc = spark
            .clone()
            .range(Some(1), 3, 1, Some(1))
            .sort([col("id").asc()]);

        let df_func_asc = spark.range(Some(1), 3, 1, Some(1)).sort([asc(col("id"))]);

        let rows_col_asc = df_col_asc.collect().await?;
        let rows_func_asc = df_func_asc.collect().await?;

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));

        let expected = RecordBatch::try_from_iter(vec![("id", id)])?;

        assert_eq!(&expected, &rows_col_asc);
        assert_eq!(&expected, &rows_func_asc);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_desc() -> Result<(), SparkError> {
        let spark = setup().await;

        let df_col_asc = spark
            .clone()
            .range(Some(1), 3, 1, Some(1))
            .sort([col("id").desc()]);

        let df_func_asc = spark.range(Some(1), 3, 1, Some(1)).sort([desc(col("id"))]);

        let rows_col_desc = df_col_asc.collect().await?;
        let rows_func_desc = df_func_asc.collect().await?;

        let id: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));

        let expected = RecordBatch::try_from_iter(vec![("id", id)])?;

        assert_eq!(&expected, &rows_col_desc);
        assert_eq!(&expected, &rows_func_desc);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_coalesce() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, None]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1), None]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone(), b.clone()])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .withColumn("coalesce", coalesce(["a", "b"]))
            .collect()
            .await?;

        println!("{:?}", res);

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("coalesce", DataType::Int64, true),
        ]);

        let c: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(1), None]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a, b, c])?;

        assert_eq!(data, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_func_input_file() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/examples/src/main/resources/people.csv"];

        let df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path);

        let res = df.select(input_file_name()).head(None).await?;

        let a: ArrayRef = Arc::new(StringArray::from(vec![
            "file:///opt/spark/examples/src/main/resources/people.csv",
        ]));

        let expected = RecordBatch::try_from_iter(vec![("input_file_name()", a)])?;

        assert_eq!(res, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_func_isnull() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone()])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .select([col("a"), isnull("a").alias("r1")])
            .collect()
            .await?;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("r1", DataType::Boolean, false),
        ]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let r1: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![a.clone(), r1])?;

        assert_eq!(res, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_func_named_struct() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        let c: ArrayRef = Arc::new(Int64Array::from(vec![3]));

        let data = RecordBatch::try_from_iter(vec![("a", a.clone()), ("b", b), ("c", c.clone())])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .select(named_struct([lit("x"), col("a"), lit("y"), col("c")]).alias("struct"))
            .collect()
            .await?;

        let struct_array: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("x", DataType::Int64, false)), a),
            (Arc::new(Field::new("y", DataType::Int64, false)), c),
        ]));

        let expected = RecordBatch::try_from_iter(vec![("struct", struct_array)])?;
        println!("{:?}", &res);

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_sqrt() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 1, 1, Some(1)).select(sqrt(lit(4)));

        let row = df.collect().await?;

        let schema = Schema::new(vec![Field::new("SQRT(4)", DataType::Float64, true)]);

        let val = Float64Array::from(vec![2.0]);

        let expected = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(val)])?;

        assert_eq!(expected, row);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_add() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![2, 3, 4]));

        let data = RecordBatch::try_from_iter(vec![("a", a)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .select((col("a") + lit(4)).alias("add"))
            .collect()
            .await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![6, 7, 8]));

        let expected = RecordBatch::try_from_iter(vec![("add", a)])?;

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_substract() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![4, 5, 6]));

        let data = RecordBatch::try_from_iter(vec![("a", a)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .select((col("a") - lit(4)).alias("sub"))
            .collect()
            .await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![0, 1, 2]));

        let expected = RecordBatch::try_from_iter(vec![("sub", a)])?;

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_multiple() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));

        let data = RecordBatch::try_from_iter(vec![("a", a)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .select((col("a") * lit(4)).alias("multi"))
            .collect()
            .await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![4, 8, 12]));

        let expected = RecordBatch::try_from_iter(vec![("multi", a)])?;

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_col_contains() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));

        let data = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .filter(col("name").contains("e"))
            .select("name")
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_col_isin() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));

        let data = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        let df = spark.createDataFrame(&data)?;

        let res = df
            .clone()
            .filter(col("name").isin(vec!["Tom", "Bob"]))
            .select("name")
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Bob"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);

        // negate isin
        let res = df
            .filter(negate(col("name").isin(vec!["Tom", "Bob"])))
            .select("name")
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);

        Ok(())
    }
}
