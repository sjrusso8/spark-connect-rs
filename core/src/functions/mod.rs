//! A re-implementation of Spark functions

use crate::expressions::VecExpression;
use crate::spark;
use crate::DataFrame;

use crate::column::Column;

use crate::spark::expression::Literal;

use rand::random;

pub fn invoke_func<I, S>(name: &str, args: I) -> Column
where
    I: IntoIterator<Item = S>,
    S: Into<Column>,
{
    Column::from(spark::Expression {
        expr_type: Some(spark::expression::ExprType::UnresolvedFunction(
            spark::expression::UnresolvedFunction {
                function_name: name.to_string(),
                arguments: VecExpression::from_iter(args).into(),
                is_distinct: false,
                is_user_defined_function: false,
            },
        )),
    })
}

/// Create a column from a &str
pub fn col(value: &str) -> Column {
    Column::from(value)
}

/// Create a column from a &str
pub fn column(value: &str) -> Column {
    Column::from(value)
}

/// Create a literal value from a rust data type
pub fn lit(col: impl Into<Literal>) -> Column {
    Column::from(col.into())
}

pub fn approx_count_distinct(col: Column, rsd: Option<f32>) -> Column {
    match rsd {
        Some(rsd) => invoke_func("approx_count_distinct", vec![col, lit(rsd)]),
        None => invoke_func("approx_count_distinct", vec![col]),
    }
}

pub fn array_append(col: Column, value: Column) -> Column {
    invoke_func("array_append", vec![col, value])
}

pub fn array_insert(col: Column, pos: Column, value: Column) -> Column {
    invoke_func("array_insert", vec![col, pos, value])
}

pub fn array_join(col: Column, delimiter: &str, null_replacement: Option<&str>) -> Column {
    match null_replacement {
        Some(replacement) => invoke_func("array_join", vec![col, lit(delimiter), lit(replacement)]),
        None => invoke_func("array_join", vec![col, lit(delimiter)]),
    }
}

pub fn array_position(col: Column, value: impl Into<spark::expression::Literal>) -> Column {
    invoke_func("array_position", vec![col, lit(value)])
}

pub fn array_remove(col: Column, element: impl Into<spark::expression::Literal>) -> Column {
    invoke_func("array_remove", vec![col, lit(element)])
}

pub fn array_repeat(col: Column, count: impl Into<spark::expression::Literal>) -> Column {
    invoke_func("array_repeat", vec![col, lit(count)])
}

pub fn array_overlap(a1: Column, a2: Column) -> Column {
    invoke_func("array_overlap", vec![a1, a2])
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
fn when(condition: Column, value: Column) -> Column {
    unimplemented!("not implemented")
}

pub fn bitwise_not<T: Into<Column>>(col: T) -> Column {
    invoke_func("~", vec![col.into()])
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

pub fn log(arg1: Column, arg2: Option<Column>) -> Column {
    match arg2 {
        Some(arg2) => invoke_func("log", vec![arg1, arg2]),
        None => ln(arg1),
    }
}

pub fn mean(col: Column) -> Column {
    avg(col)
}

pub fn ntile(n: i32) -> Column {
    invoke_func("ntitle", vec![lit(n)])
}

pub fn negate(col: Column) -> Column {
    invoke_func("negative", vec![col])
}

pub fn pow(col1: Column, col2: Column) -> Column {
    power(col1, col2)
}

pub fn round(col: Column, scale: Option<f32>) -> Column {
    let values = vec![col, lit(scale.unwrap_or(0.0)).clone()];
    invoke_func("round", values)
}

pub fn add_months(start: Column, months: Column) -> Column {
    invoke_func("add_months", vec![start, months])
}

pub fn date_add(start: Column, days: Column) -> Column {
    invoke_func("date_add", vec![start, days])
}

pub fn dateadd(start: Column, days: Column) -> Column {
    invoke_func("dateadd", vec![start, days])
}

pub fn datediff(end: Column, start: Column) -> Column {
    invoke_func("datediff", vec![end, start])
}

pub fn date_sub(start: Column, end: Column) -> Column {
    invoke_func("date_sub", vec![start, end])
}

pub fn character_length(str: Column) -> Column {
    invoke_func("character_length", vec![str])
}

pub fn char_length(str: Column) -> Column {
    invoke_func("char_length", vec![str])
}

pub fn ucase(str: Column) -> Column {
    invoke_func("ucase", vec![str])
}

pub fn asc(col: Column) -> Column {
    col.asc()
}

pub fn asc_nulls_first(col: Column) -> Column {
    col.asc_nulls_first()
}

pub fn asc_nulls_last(col: Column) -> Column {
    col.asc_nulls_last()
}

pub fn desc(col: Column) -> Column {
    col.desc()
}

pub fn desc_nulls_first(col: Column) -> Column {
    col.desc_nulls_first()
}

pub fn desc_nulls_last(col: Column) -> Column {
    col.desc_nulls_last()
}

macro_rules! generate_functions {
    (no_args: $($func_name:ident),*) => {
        $(
            pub fn $func_name() -> Column {
                let empty_args: Vec<Column> = vec![];
                invoke_func(stringify!($func_name), empty_args)
            }
        )*
    };
    (one_col: $($func_name:ident),*) => {
        $(
            pub fn $func_name(col: impl Into<Column>) -> Column
            {
                invoke_func(stringify!($func_name), vec![col.into()])
            }
        )*
    };
    (two_cols: $($func_name:ident),*) => {
        $(
            pub fn $func_name<T: Into<Column>>(col1: T, col2: T) -> Column
            {
                invoke_func(stringify!($func_name), vec![col1.into(), col2.into()])
            }
        )*
    };
    (multiple_cols: $($func_name:ident),*) => {
        $(
            pub fn $func_name<I>(cols: I) -> Column
            where
                I: IntoIterator<Item = Column>,
            {
                invoke_func(stringify!($func_name), cols)
            }
        )*
    };
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
    isnotnull,
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
    group_id,
    struct_col
);

#[cfg(test)]
mod tests {

    use super::*;

    use core::f64;
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::{errors::SparkError, window::Window};
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
    async fn test_func_alias() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        let data = RecordBatch::try_from_iter(vec![("name", name.clone()), ("age", age.clone())])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("name").alias("new_name"), col("age").alias("new_age")])
            .collect()
            .await?;

        let schema = Schema::new(vec![
            Field::new("new_name", DataType::Utf8, false),
            Field::new("new_age", DataType::Int64, false),
        ]);

        let new_name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let new_age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![new_name, new_age])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_lit() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1, 1, Some(1))
            .select([lit(5).alias("height"), col("id")]);

        let row = df.collect().await?;

        let height: ArrayRef = Arc::new(Int32Array::from(vec![5]));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![0]));

        let expected = RecordBatch::try_from_iter(vec![("height", height), ("id", id)])?;

        assert_eq!(expected.clone(), row);

        let df = spark
            .range(None, 1, 1, Some(1))
            .select([lit(vec![1, 2, 3])]);

        let row = df.collect().await?;

        assert_eq!(1, row.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn test_func_asc() -> Result<(), SparkError> {
        let spark = setup().await;

        let df_col_asc = spark.range(Some(1), 3, 1, Some(1)).sort([col("id").asc()]);

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
    async fn test_func_asc_nulls_first() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, None]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone()])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("a")])
            .sort([col("a").asc_nulls_first()])
            .collect()
            .await?;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let b: ArrayRef = Arc::new(Int64Array::from(vec![None, None, Some(1)]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![b.clone()])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_func_asc_nulls_last() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![None, None, Some(1)]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone()])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("a")])
            .sort([col("a").asc_nulls_last()])
            .collect()
            .await?;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let b: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, None]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![b.clone()])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_func_desc() -> Result<(), SparkError> {
        let spark = setup().await;

        let df_col_asc = spark.range(Some(1), 3, 1, Some(1)).sort([col("id").desc()]);

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
    async fn test_func_desc_nulls_first() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            None,
        ]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone()])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("a")])
            .sort([col("a").desc_nulls_first()])
            .collect()
            .await?;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let b: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            None,
            Some(3),
            Some(2),
            Some(1),
        ]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![b.clone()])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_func_desc_nulls_last() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            None,
        ]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone()])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("a")])
            .sort([col("a").desc_nulls_last()])
            .collect()
            .await?;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let b: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(3),
            Some(2),
            Some(1),
            None,
            None,
        ]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![b.clone()])?;

        assert_eq!(expected, res);

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

        let df = spark.create_dataframe(&data)?;

        let res = df
            .with_column("coalesce", coalesce([col("a"), col("b")]))
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

        let path = ["/opt/spark/work-dir/datasets/people.csv"];

        let df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path)?;

        let res = df.select([input_file_name()]).head(None).await?;

        let a: ArrayRef = Arc::new(StringArray::from(vec![
            "file:///opt/spark/work-dir/datasets/people.csv",
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

        let df = spark.create_dataframe(&data)?;

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

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([named_struct([lit("x"), col("a"), lit("y"), col("c")]).alias("struct")])
            .collect()
            .await?;

        let struct_array: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("x", DataType::Int64, false)), a),
            (Arc::new(Field::new("y", DataType::Int64, false)), c),
        ]));

        let expected = RecordBatch::try_from_iter(vec![("struct", struct_array)])?;

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_sqrt() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 1, 1, Some(1)).select([sqrt(lit(4))]);

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

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([(col("a") + lit(4)).alias("add")])
            .collect()
            .await?;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![6, 7, 8]));

        let expected = RecordBatch::try_from_iter(vec![("add", a)])?;

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_subtract() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![4, 5, 6]));

        let data = RecordBatch::try_from_iter(vec![("a", a)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([(col("a") - lit(4)).alias("sub")])
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

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([(col("a") * lit(4)).alias("multi")])
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

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("name").contains(lit("e")))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_startswith() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));

        let data = RecordBatch::try_from_iter(vec![("name", name)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("name").startswith(lit("Al")))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_endswith() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));

        let data = RecordBatch::try_from_iter(vec![("name", name)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("name").endswith(lit("ice")))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_like() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));

        let data = RecordBatch::try_from_iter(vec![("name", name)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("name").like(lit("Alice")))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_ilike() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));

        let data = RecordBatch::try_from_iter(vec![("name", name)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("name").ilike(lit("%Ice")))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_rlike() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));

        let data = RecordBatch::try_from_iter(vec![("name", name)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("name").rlike(lit("ice$")))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_eq() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4]));

        let data = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df.select([col("a").eq(col("b"))]).collect().await?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false]));

        let expected = RecordBatch::try_from_iter(vec![("(a = b)", a)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_and() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
        let gender: ArrayRef = Arc::new(StringArray::from(vec!["M", "F", "M"]));

        let data =
            RecordBatch::try_from_iter(vec![("name", name), ("age", age), ("gender", gender)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("age").eq(lit(23)).and(col("gender").eq(lit("F"))))
            .select(vec!["name", "gender"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));
        let gender: ArrayRef = Arc::new(StringArray::from(vec!["F"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name), ("gender", gender)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_or() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));
        let gender: ArrayRef = Arc::new(StringArray::from(vec!["M", "F", "M"]));

        let data =
            RecordBatch::try_from_iter(vec![("name", name), ("age", age), ("gender", gender)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .filter(col("age").eq(lit(23)).or(col("age").eq(lit(16))))
            .select(vec!["name", "age"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![23, 16]));

        let expected = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_is_not_null() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1)]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone()])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("a"), isnotnull("a").alias("r1")])
            .collect()
            .await?;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("r1", DataType::Boolean, false),
        ]);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1)]));
        let r1: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![a.clone(), r1])?;

        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_isnan() -> Result<(), SparkError> {
        let spark = setup().await;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
        ]);

        let a: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), Some(f64::NAN)]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(1.0)]));

        let data = RecordBatch::try_new(Arc::new(schema), vec![a.clone(), b.clone()])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select(vec![
                col("a"),
                col("b"),
                isnan("a").alias("r1"),
                isnan("b").alias("r2"),
            ])
            .collect()
            .await?;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("r1", DataType::Boolean, false),
            Field::new("r2", DataType::Boolean, false),
        ]);

        let r1: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
        let r2: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));

        let expected = RecordBatch::try_new(
            Arc::new(schema),
            vec![a.clone(), b.clone(), r1.clone(), r2.clone()],
        )?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_col_isin() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![14, 23, 16]));

        let data = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .clone()
            .filter(col("name").isin(vec![lit("Tom"), lit("Bob")]))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Tom", "Bob"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);

        // Logical NOT for column ISIN
        let res = df
            .filter(!col("name").isin(vec![lit("Tom"), lit("Bob")]))
            .select(["name"])
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));

        let expected = RecordBatch::try_from_iter(vec![("name", name)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_col_expr() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));

        let data = RecordBatch::try_from_iter(vec![("name", name.clone())])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("name"), expr("length(name)")])
            .collect()
            .await?;

        let length: ArrayRef = Arc::new(Int32Array::from(vec![5, 3]));

        let expected = RecordBatch::try_from_iter(vec![("name", name), ("length(name)", length)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_greatest() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![4]));
        let c: ArrayRef = Arc::new(Int64Array::from(vec![4]));

        let data = RecordBatch::try_from_iter(vec![("a", a), ("b", b.clone()), ("c", c)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([greatest([col("a"), col("b"), col("c")])])
            .collect()
            .await?;

        let expected = RecordBatch::try_from_iter(vec![("greatest(a, b, c)", b)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_least() -> Result<(), SparkError> {
        let spark = setup().await;

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![4]));
        let c: ArrayRef = Arc::new(Int64Array::from(vec![4]));

        let data = RecordBatch::try_from_iter(vec![("a", a.clone()), ("b", b), ("c", c)])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([least([col("a"), col("b"), col("c")])])
            .collect()
            .await?;

        let expected = RecordBatch::try_from_iter(vec![("least(a, b, c)", a)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_col_drop_fields() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 1, 1, None).select([named_struct([
            lit("a"),
            lit(1),
            lit("b"),
            lit(2),
            lit("c"),
            lit(3),
            lit("d"),
            lit(4),
        ])
        .alias("struct_col")]);

        let df = df.select([col("struct_col")
            .drop_fields(["b", "c"])
            .alias("struct_col")]);

        let res = df.collect().await?;

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let d: ArrayRef = Arc::new(Int32Array::from(vec![4]));

        let struct_array: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, false)), a),
            (Arc::new(Field::new("d", DataType::Int32, false)), d),
        ]));

        let expected = RecordBatch::try_from_iter(vec![("struct_col", struct_array)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_col_with_field() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.range(None, 1, 1, None).select([named_struct([
            lit("a"),
            lit(1),
            lit("b"),
            lit(2),
        ])
        .alias("struct_col")]);

        let df = df.select([col("struct_col")
            .with_field("b", lit(4))
            .alias("struct_col")]);

        let res = df.collect().await?;

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![4]));

        let struct_array: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, false)), a),
            (Arc::new(Field::new("b", DataType::Int32, false)), b),
        ]));

        let expected = RecordBatch::try_from_iter(vec![("struct_col", struct_array)])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_substr() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        let data = RecordBatch::try_from_iter(vec![("age", age.clone()), ("name", name.clone())])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("name").substr(lit(1), lit(3)).alias("col")])
            .collect()
            .await?;

        let col: ArrayRef = Arc::new(StringArray::from(vec!["Ali", "Bob"]));

        let schema = Schema::new(vec![Field::new("col", DataType::Utf8, false)]);

        let expected = RecordBatch::try_new(Arc::new(schema), vec![col])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_cast() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        let data = RecordBatch::try_from_iter(vec![("age", age.clone()), ("name", name.clone())])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .select([col("age").cast("string").alias("ages")])
            .collect()
            .await?;

        let ages: ArrayRef = Arc::new(StringArray::from(vec!["2", "5"]));

        let schema = Schema::new(vec![Field::new("ages", DataType::Utf8, false)]);

        let expected = RecordBatch::try_new(Arc::new(schema), vec![ages])?;

        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    async fn test_func_over() -> Result<(), SparkError> {
        let spark = setup().await;

        let window = Window::new()
            .partition_by([col("name")])
            .order_by([col("age")])
            .rows_between(Window::unbounded_preceding(), Window::current_row());

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        let data = RecordBatch::try_from_iter(vec![("age", age.clone()), ("name", name.clone())])?;

        let df = spark.create_dataframe(&data)?;

        let res = df
            .with_column("rank", rank().over(window.clone()))
            .with_column("min", min("age").over(window))
            .sort([col("age").desc()])
            .collect()
            .await?;

        let schema = Schema::new(vec![
            Field::new("age", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("rank", DataType::Int32, false),
            Field::new("min", DataType::Int64, true),
        ]);

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Bob", "Alice"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![5, 2]));
        let rank: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
        let min: ArrayRef = Arc::new(Int64Array::from(vec![5, 2]));

        let expected = RecordBatch::try_new(Arc::new(schema), vec![age, name, rank, min])?;

        assert_eq!(expected, res);
        Ok(())
    }
}
