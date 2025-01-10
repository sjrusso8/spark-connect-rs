//! A re-implementation of Spark functions

use std::collections::HashMap;

use crate::expressions::VecExpression;
use crate::spark;
use crate::DataFrame;

use crate::column::Column;

use crate::spark::expression::Literal;

use rand::random;

pub(crate) fn invoke_func<I, S>(name: &str, args: I) -> Column
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

macro_rules! gen_func {
    // Case with no args
    ($func_name:ident, [], $doc:expr) => {
        #[doc = $doc]
        pub fn $func_name() -> Column {
            let empty_args: Vec<Column> = vec![];
            invoke_func(stringify!($func_name), empty_args)
        }
    };

    // case for any iterable of cols as a single argument
    ($func_name:ident, [cols : $param_type:ty ], $doc:expr) => {
        #[doc = $doc]
        pub fn $func_name<I>(cols: I) -> Column
        where
            I: IntoIterator,
            I::Item: Into<Column>,
        {
            invoke_func(stringify!($func_name), cols)
        }
    };

    ($func_name:ident, [$( $param_name:ident : Column ),+], $doc:expr) => {
           #[doc = $doc]
           pub fn $func_name($( $param_name : impl Into<Column> ),+) -> Column {

               invoke_func(stringify!($func_name), vec![$( $param_name.into() ),+])
            }
    };
}

pub(crate) fn options_to_map<I, K, V>(cols: I) -> Column
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let map: Vec<Column> = cols
        .into_iter()
        .flat_map(|(k, v)| vec![lit(k.as_ref()), lit(v.as_ref())])
        .collect();

    create_map(map)
}

// Normal Functions

/// Returns a [Column] based on the given column name.
pub fn col(value: impl Into<Column>) -> Column {
    value.into()
}

/// Returns a [Column] based on the given column name.
pub fn column(value: impl Into<Column>) -> Column {
    value.into()
}

/// Creates a [Column] of [spark::expression::Literal] value.
pub fn lit(col: impl Into<Literal>) -> Column {
    Column::from(col.into())
}

/// Marks a DataFrame as small enough for use in broadcast joins.
pub fn broadcast(df: DataFrame) -> DataFrame {
    df.hint::<Vec<String>>("broadcast", None)
}

gen_func!(coalesce, [cols: _], "Returns the first column that is not null.");
gen_func!(
    input_file_name,
    [],
    "Creates a string column for the file name of the current Spark task."
);

gen_func!(isnan, [col: Column], "An expression that returns true if the column is NaN.");
gen_func!(isnull, [col: Column], "An expression that returns true if the column is null");
gen_func!(
    monotonically_increasing_id,
    [],
    "A column that generates monotonically increasing 64-bit integers."
);
gen_func!(named_struct, [cols: _], "Creates a struct with the given field names and values.");

gen_func!(nanvl, [col1: Column, col2: Column], "Returns col1 if it is not NaN, or col2 if col1 is NaN.");

/// Generates a random column with independent and identically distributed (i.i.d.) samples uniformly distributed in [0.0, 1.0).
pub fn rand(seed: Option<i32>) -> Column {
    invoke_func("rand", vec![lit(seed.unwrap_or(random::<i32>()))])
}

/// Generates a column with independent and identically distributed (i.i.d.) samples from the standard normal distribution.
pub fn randn(seed: Option<i32>) -> Column {
    invoke_func("randn", vec![lit(seed.unwrap_or(random::<i32>()))])
}

gen_func!(spark_partition_id, [], "A column for partition ID.");

#[allow(dead_code)]
#[allow(unused_variables)]
/// Evaluates a list of conditions and returns one of multiple possible result expressions.
fn when(condition: impl Into<Column>, value: Column) -> Column {
    unimplemented!("not implemented")
}

/// Computes bitwise not.
pub fn bitwise_not(col: impl Into<Column>) -> Column {
    invoke_func("~", vec![col.into()])
}

/// Parses the expression string into the column that it represents
pub fn expr(val: &str) -> Column {
    Column::from(spark::Expression {
        expr_type: Some(spark::expression::ExprType::ExpressionString(
            spark::expression::ExpressionString {
                expression: val.to_string(),
            },
        )),
    })
}

// Math Functions

gen_func!(sqrt, [col: Column], "Computes the square root of the specified float value.");
gen_func!(abs, [col: Column], "Computes the absolute value.");
gen_func!(least, [cols: _], "Returns the least value of the list of column names, skipping null values.");
gen_func!(greatest, [cols: _], "Returns the greatest value of the list of column names, skipping null values.");
gen_func!(acos, [col: Column], "Computes inverse cosine of the input column.");
gen_func!(acosh, [col: Column], "Computes inverse hyperbolic cosine of the input column.");
gen_func!(asin, [col: Column], "Computes inverse sine of the input column.");
gen_func!(asinh, [col: Column], "Computes inverse hyperbolic sine of the input column.");
gen_func!(atan, [col: Column], "Compute inverse tangent of the input column.");
gen_func!(atanh, [col: Column], "Computes inverse hyperbolic tangent of the input column.");
gen_func!(atan2, [col1: Column, col2: Column], "Computes inverse hyperbolic tangent of the input columns.");
gen_func!(bin, [col: Column], "Returns the string representation of the binary value of the given column.");
gen_func!(cbrt, [col: Column], "Computes the cube-root of the given value.");
gen_func!(ceil, [col: Column], "Computes the ceiling of the given value.");
gen_func!(ceiling, [col: Column], "Computes the ceiling of the given value.");

/// Convert a number in a string column from one base to another.
pub fn conv(col: impl Into<Column>, from_base: i32, to_base: i32) -> Column {
    invoke_func("conv", vec![col.into(), lit(from_base), lit(to_base)])
}
gen_func!(cos, [col: Column], "Computes cosine of the input column.");
gen_func!(cosh, [col: Column], "Computes hyperbolic cosine of the input column.");
gen_func!(cot, [col: Column], "Computes cotangent of the input column.");
gen_func!(csc, [col: Column], "Computes cosecant of the input column.");
gen_func!(e, [], "Returns Euler’s number.");
gen_func!(exp, [col: Column], "Computes the exponential of the given value.");
gen_func!(expm1, [col: Column], "Computes the exponential of the given value minus one.");
gen_func!(factorial, [col: Column], "Computes the factorial of the given value.");
gen_func!(floor, [col: Column], "Computes the floor of the given value.");
gen_func!(hex, [col: Column], "Computes hex value of the given column");
gen_func!(unhex, [col: Column], "Inverse of hex.");
gen_func!(hypot, [col1: Column, col2: Column], "Computes sqrt(a^2 + b^2) without intermediate overflow or underflow.");
gen_func!(ln, [col: Column], "Returns the natural logarithm of the argument.");

/// Returns the first argument-based logarithm of the second argument.
pub fn log(arg1: impl Into<Column>, arg2: Option<impl Into<Column>>) -> Column {
    match arg2 {
        Some(arg2) => invoke_func("log", vec![arg1.into(), arg2.into()]),
        None => ln(arg1),
    }
}

gen_func!(log10, [col: Column], "Computes the logarithm of the given value in Base 10.");
gen_func!(log1p, [col: Column], "Computes the natural logarithm of the “given value plus one”.");
gen_func!(log2, [col: Column], "Returns the base-2 logarithm of the argument.");
gen_func!(negative, [col: Column], "Returns the negative value.");

/// Returns the negative value.
pub fn negate(col: impl Into<Column>) -> Column {
    invoke_func("negative", vec![col])
}

gen_func!(pi, [], "Returns Pi.");
gen_func!(pmod, [dividend: Column, divisor: Column], "Returns the positive value of dividend mod divisor.");
gen_func!(power, [col1: Column, col2: Column], "Returns the value of the first argument raised to the power of the second argument.");
gen_func!(positive, [col: Column], "Returns the value.");

/// Returns the value of the first argument raised to the power of the second argument.
pub fn pow(col1: impl Into<Column>, col2: impl Into<Column>) -> Column {
    power(col1.into(), col2.into())
}

gen_func!(rint, [col: Column], "Returns the double value that is closest in value to the argument and is equal to a mathematical integer.");

/// Round the given value to scale decimal places using HALF_UP rounding mode if scale >= 0 or at integral part when scale < 0.
pub fn round(col: impl Into<Column>, scale: Option<f32>) -> Column {
    let values = vec![col.into(), lit(scale.unwrap_or(0.0)).clone()];
    invoke_func("round", values)
}

/// Round the given value to scale decimal places using HALF_EVEN rounding mode if scale >= 0 or at integral part when scale < 0.
pub fn bround(col: impl Into<Column>, scale: Option<f32>) -> Column {
    let values = vec![col.into(), lit(scale.unwrap_or(0.0)).clone()];
    invoke_func("bround", values)
}

gen_func!(sec, [col: Column], "Computes secant of the input column.");
gen_func!(shiftleft, [col: Column, num_bits: Column], "Shift the given value numBits left.");
gen_func!(shiftright, [col: Column, num_bits: Column], "(Signed) shift the given value numBits right.");
gen_func!(shiftrightunsigned, [col: Column, num_bits: Column], "(Signed) shift the given value numBits right.");
gen_func!(sign, [col: Column], "Computes the signum of the given value.");
gen_func!(signum, [col: Column], "Computes the signum of the given value.");
gen_func!(sin, [col: Column], "Computes sine of the input column.");
gen_func!(sinh, [col: Column], "Computes hyperbolic sine of the input column.");
gen_func!(tan, [col: Column], "Computes tangent of the input column.");
gen_func!(tanh, [col: Column], "Computes hyperbolic tangent of the input column.");
gen_func!(try_add, [left: Column, right: Column], "Returns the sum of left and right and the result is null on overflow.");
gen_func!(try_avg, [col: Column], "Returns the mean calculated from values of a group and the result is null on overflow.");
gen_func!(try_divide, [left: Column, right: Column], "Returns dividend/divisor.");
gen_func!(try_multiply, [left: Column, right: Column], "Returns left*right and the result is null on overflow.");
gen_func!(try_subtract, [left: Column, right: Column], "Returns left-right and the result is null on overflow.");
gen_func!(try_sum, [left: Column, right: Column], "Returns sum calculated from values of a group and the result is null on overflow.");

gen_func!(degrees, [col: Column], "Converts an angle measured in radians to an approximately equivalent angle measured in degrees.");
gen_func!(radians, [col: Column], "Converts an angle measured in degrees to an approximately equivalent angle measured in radians.");

/// Returns the bucket number into which the value of this expression would fall after being evaluated.
pub fn width_bucket(
    v: impl Into<Column>,
    min: impl Into<Column>,
    max: impl Into<Column>,
    num_bucket: impl Into<Column>,
) -> Column {
    invoke_func(
        "width_bucket",
        vec![v.into(), min.into(), max.into(), num_bucket.into()],
    )
}

// Datetime Functions

gen_func!(add_months, [start: Column, months: Column], "Returns the date that is months months after start.");

/// Converts the timestamp without time zone sourceTs from the sourceTz time zone to targetTz.
pub fn convert_timezone(
    source_tz: Option<impl Into<Column>>,
    target_tz: impl Into<Column>,
    source_ts: impl Into<Column>,
) -> Column {
    match source_tz {
        Some(source_tz) => invoke_func(
            "convert_timezone",
            vec![source_tz.into(), target_tz.into(), source_ts.into()],
        ),
        None => invoke_func("convert_timezone", vec![target_tz.into(), source_ts.into()]),
    }
}

gen_func!(
    curdate,
    [],
    "Returns the current date at the start of query evaluation as a DateType column."
);
gen_func!(
    current_date,
    [],
    "Returns the current date at the start of query evaluation as a DateType column."
);
gen_func!(
    current_timestamp,
    [],
    "Returns the current timestamp at the start of query evaluation as a TimestampType column."
);
gen_func!(
    current_timezone,
    [],
    "Returns the current session local timezone."
);

gen_func!(date_add, [start: Column, days: Column], "Returns the date that is days days after start.");
gen_func!(date_diff, [end: Column, start: Column], "Returns the number of days from start to end.");
gen_func!(date_format, [date: Column, format: Column], "Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument.");

gen_func!(date_from_unix_date, [days: Column], "Create date from the number of days since 1970-01-01.");

gen_func!(date_sub, [start: Column, days: Column], "Returns the date that is days days before start.");
gen_func!(date_trunc, [format: Column, timestamp: Column], "Returns timestamp truncated to the unit specified by the format.");
gen_func!(dateadd, [start: Column, days: Column], "Returns the date that is days days after start.");
gen_func!(datediff, [end: Column, start: Column], "Returns the number of days from start to end.");
gen_func!(day, [col: Column], "Extract the day of the month of a given date/timestamp as integer.");
gen_func!(date_part, [field: Column, source: Column], "Extracts a part of the date/timestamp or interval source.");
gen_func!(dayofmonth, [col: Column], "Extract the day of the month of a given date/timestamp as integer.");
gen_func!(dayofweek, [col: Column], "Extract the day of the week of a given date/timestamp as integer.");
gen_func!(dayofyear, [col: Column], "Extract the day of the year of a given date/timestamp as integer.");
gen_func!(extract, [field: Column, source: Column], "Extracts a part of the date/timestamp or interval source.");
gen_func!(second, [col: Column], "Extract the seconds of a given date as integer.");
gen_func!(weekofyear, [col: Column], "Extract the week number of a given date as integer.");
gen_func!(year, [col: Column], "Extract the year of a given date/timestamp as integer.");
gen_func!(quarter, [col: Column], "Extract the quarter of a given date/timestamp as integer.");
gen_func!(month, [col: Column], "Extract the month of a given date/timestamp as integer.");
gen_func!(last_day, [date: Column], "Returns the last day of the month which the given date belongs to.");
gen_func!(localtimestamp, [], "Returns the current timestamp without time zone at the start of query evaluation as a timestamp without time zone column.");

/// Make DayTimeIntervalType duration from days, hours, mins and secs.
pub fn make_dt_interval(
    days: Option<impl Into<Column>>,
    hours: Option<impl Into<Column>>,
    mins: Option<impl Into<Column>>,
    secs: Option<impl Into<Column>>,
) -> Column {
    let _days = match days {
        Some(d) => d.into(),
        None => lit(0),
    };

    let _hours = match hours {
        Some(h) => h.into(),
        None => lit(0),
    };

    let _mins = match mins {
        Some(m) => m.into(),
        None => lit(0),
    };

    let _secs = match secs {
        Some(s) => s.into(),
        None => lit(0.0),
    };

    invoke_func("make_dt_interval", vec![_days, _hours, _mins, _secs])
}

/// Make interval from years, months, weeks, days, hours, mins and secs.
pub fn make_interval(
    years: Option<impl Into<Column>>,
    months: Option<impl Into<Column>>,
    weeks: Option<impl Into<Column>>,
    days: Option<impl Into<Column>>,
    hours: Option<impl Into<Column>>,
    mins: Option<impl Into<Column>>,
    secs: Option<impl Into<Column>>,
) -> Column {
    // Assign default values if None
    let _years = match years {
        Some(y) => y.into(),
        None => lit(0),
    };

    let _months = match months {
        Some(m) => m.into(),
        None => lit(0),
    };

    let _weeks = match weeks {
        Some(w) => w.into(),
        None => lit(0),
    };

    let _days = match days {
        Some(d) => d.into(),
        None => lit(0),
    };

    let _hours = match hours {
        Some(h) => h.into(),
        None => lit(0),
    };

    let _mins = match mins {
        Some(m) => m.into(),
        None => lit(0),
    };

    let _secs = match secs {
        Some(s) => s.into(),
        None => lit(0.0),
    };

    invoke_func(
        "make_interval",
        vec![_years, _months, _weeks, _days, _hours, _mins, _secs],
    )
}

/// Create timestamp from years, months, days, hours, mins, secs and timezone fields.
pub fn make_timestamp(
    years: impl Into<Column>,
    months: impl Into<Column>,
    days: impl Into<Column>,
    hours: impl Into<Column>,
    mins: impl Into<Column>,
    timezone: Option<impl Into<Column>>,
) -> Column {
    match timezone {
        Some(tz) => invoke_func(
            "make_timestamp",
            vec![
                years.into(),
                months.into(),
                days.into(),
                hours.into(),
                mins.into(),
                tz.into(),
            ],
        ),
        None => invoke_func(
            "make_timestamp",
            vec![
                years.into(),
                months.into(),
                days.into(),
                hours.into(),
                mins.into(),
            ],
        ),
    }
}

/// Create the current timestamp with local time zone from years, months, days, hours, mins, secs and timezone fields.
pub fn make_timestamp_ltz(
    years: impl Into<Column>,
    months: impl Into<Column>,
    days: impl Into<Column>,
    hours: impl Into<Column>,
    mins: impl Into<Column>,
    timezone: Option<impl Into<Column>>,
) -> Column {
    match timezone {
        Some(tz) => invoke_func(
            "make_timestamp_ltz",
            vec![
                years.into(),
                months.into(),
                days.into(),
                hours.into(),
                mins.into(),
                tz.into(),
            ],
        ),
        None => invoke_func(
            "make_timestamp_ltz",
            vec![
                years.into(),
                months.into(),
                days.into(),
                hours.into(),
                mins.into(),
            ],
        ),
    }
}

/// Create local date-time from years, months, days, hours, mins, secs fields.
pub fn make_timestamp_ntz(
    years: impl Into<Column>,
    months: impl Into<Column>,
    days: impl Into<Column>,
    hours: impl Into<Column>,
    mins: impl Into<Column>,
) -> Column {
    invoke_func(
        "make_timestamp_ntz",
        vec![
            years.into(),
            months.into(),
            days.into(),
            hours.into(),
            mins.into(),
        ],
    )
}

/// Make year-month interval from years, months.
pub fn make_ym_interval(
    years: Option<impl Into<Column>>,
    months: Option<impl Into<Column>>,
) -> Column {
    // Assign default values if None
    let _years = match years {
        Some(y) => y.into(),
        None => lit(0),
    };

    let _months = match months {
        Some(m) => m.into(),
        None => lit(0),
    };

    invoke_func("make_ym_interval", vec![_years, _months])
}

gen_func!(minute, [col: Column], "Extract the minutes of a given timestamp as integer.");

/// Returns number of months between dates date1 and date2.
pub fn months_between(
    date1: impl Into<Column>,
    date2: impl Into<Column>,
    round_off: Option<bool>,
) -> Column {
    match round_off {
        Some(roff) => invoke_func(
            "months_between",
            vec![date1.into(), date2.into(), lit(roff)],
        ),
        None => invoke_func(
            "months_between",
            vec![date1.into(), date2.into(), lit(true)],
        ),
    }
}

gen_func!(next_day, [date: Column, day_of_week: Column], "Returns the first date which is later than the value of the date column based on second week day argument.");
gen_func!(hour, [col: Column], "Extract the hours of a given timestamp as integer.");
gen_func!(make_date, [year: Column, month: Column, day: Column], "Returns a column with a date built from the year, month and day columns.");
gen_func!(
    now,
    [],
    "Returns the current timestamp at the start of query evaluation."
);

/// Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a
/// string representing the timestamp of that moment in the current system time zone
/// in the given format.
pub fn from_unixtime(timestamp: impl Into<Column>, format: Option<&str>) -> Column {
    let format = match format {
        Some(f) => lit(f),
        None => lit("yyyy-MM-dd HH:mm:ss"),
    };

    invoke_func("from_unixtime", vec![timestamp.into(), format])
}

/// Convert time string with given pattern (‘yyyy-MM-dd HH:mm:ss’, by default)
/// to Unix time stamp (in seconds), using the default timezone and the default locale,
/// returns null if failed.
pub fn unix_timestamp(timestamp: Option<impl Into<Column>>, format: Option<&str>) -> Column {
    let format = match format {
        Some(f) => lit(f),
        None => lit("yyyy-MM-dd HH:mm:ss"),
    };

    match timestamp {
        Some(ts) => invoke_func("unix_timestamp", vec![ts.into(), format]),
        None => {
            let empty_args: Vec<Column> = vec![];
            invoke_func("unix_timestamp", empty_args)
        }
    }
}

/// Returns the UNIX timestamp of the given time.
pub fn to_unix_timestamp(timestamp: impl Into<Column>, format: Option<&str>) -> Column {
    match format {
        Some(f) => invoke_func("to_unix_timestamp", vec![timestamp.into(), lit(f)]),
        None => invoke_func("to_unix_timestamp", vec![timestamp.into()]),
    }
}

/// Converts a Column into pyspark.sql.types.TimestampType using the optionally
/// specified format.
pub fn to_timestamp(timestamp: impl Into<Column>, format: Option<&str>) -> Column {
    match format {
        Some(f) => invoke_func("to_timestamp", vec![timestamp.into(), lit(f)]),
        None => invoke_func("to_timestamp", vec![timestamp.into()]),
    }
}

/// Parses the timestamp with the format to a timestamp without time zone.
pub fn to_timestamp_ltz(timestamp: impl Into<Column>, format: Option<&str>) -> Column {
    match format {
        Some(f) => invoke_func("to_timestamp_ltz", vec![timestamp.into(), lit(f)]),
        None => invoke_func("to_timestamp_ltz", vec![timestamp.into()]),
    }
}

/// Parses the timestamp with the format to a timestamp without time zone.
pub fn to_timestamp_ntz(timestamp: impl Into<Column>, format: Option<&str>) -> Column {
    match format {
        Some(f) => invoke_func("to_timestamp_ntz", vec![timestamp.into(), lit(f)]),
        None => invoke_func("to_timestamp_ntz", vec![timestamp.into()]),
    }
}
/// Parses the timestamp with the format to a timestamp without time zone.
pub fn to_date(col: impl Into<Column>, format: Option<&str>) -> Column {
    match format {
        Some(f) => invoke_func("to_date", vec![col.into(), lit(f)]),
        None => invoke_func("to_date", vec![col.into()]),
    }
}
gen_func!(trunc, [col: Column, format: Column], "Returns date truncated to the unit specified by the format.");
gen_func!(from_utc_timestamp, [timestamp: Column, tz: Column], "This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE.");
gen_func!(to_utc_timestamp, [timestamp: Column, tz: Column], "This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE.");
gen_func!(weekday, [col: Column], "Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, …, 6 = Sunday).");

/// Bucketize rows into one or moe time windows given a timestamp specifying column.
pub fn window(
    time_column: impl Into<Column>,
    window_duration: &str,
    slide_duration: Option<&str>,
    start_time: Option<&str>,
) -> Column {
    let window_duration = lit(window_duration);

    if slide_duration.is_some() & start_time.is_some() {
        invoke_func(
            "window",
            vec![
                time_column.into(),
                window_duration,
                lit(slide_duration.unwrap()),
                lit(start_time.unwrap()),
            ],
        )
    } else if slide_duration.is_some() & start_time.is_none() {
        invoke_func(
            "window",
            vec![
                time_column.into(),
                window_duration,
                lit(slide_duration.unwrap()),
            ],
        )
    } else if slide_duration.is_none() & start_time.is_some() {
        invoke_func(
            "window",
            vec![
                time_column.into(),
                window_duration,
                lit(start_time.unwrap()),
            ],
        )
    } else {
        invoke_func("window", vec![time_column.into(), window_duration])
    }
}

gen_func!(session_window, [time_column: Column, gap_duration: Column], "Generates session window given a timestamp specifying column.");
gen_func!(timestamp_micros, [col: Column], "Creates timestamp from the number of microseconds since UTC epoch.");
gen_func!(timestamp_millis, [col: Column], "Creates timestamp from the number of milliseconds since UTC epoch.");
gen_func!(timestamp_seconds, [col: Column], "Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z) to a timestamp.");

pub fn try_to_timestamp(col: impl Into<Column>, format: Option<impl Into<Column>>) -> Column {
    match format {
        Some(val) => invoke_func("try_to_timestamp", vec![col.into(), val.into()]),
        None => invoke_func("try_to_timestamp", vec![col.into()]),
    }
}

gen_func!(unix_date, [col: Column], "Returns the number of days since 1970-01-01.");
gen_func!(unix_millis, [col: Column], "Returns the number of milliseconds since 1970-01-01 00:00:00 UTC.");
gen_func!(unix_micros, [col: Column], "Returns the number of microseconds since 1970-01-01 00:00:00 UTC.");
gen_func!(unix_seconds, [col: Column], "Returns the number of seconds since 1970-01-01 00:00:00 UTC.");
gen_func!(window_time, [window_col: Column], "Computes the event time from a window column.");

// Collection Functions

gen_func!(array, [cols: _], "Creates a new array column.");
gen_func!(array_contains, [col: Column, value: Column], "Returns null if the array is null, true if the array contains the given value, and false otherwise.");
gen_func!(arrays_overlap, [a1: Column, a2: Column], "Returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.");

/// Concatenates the elements of column using the delimiter.
pub fn array_join(
    col: impl Into<Column>,
    delimiter: &str,
    null_replacement: Option<&str>,
) -> Column {
    match null_replacement {
        Some(replacement) => invoke_func(
            "array_join",
            vec![col.into(), lit(delimiter), lit(replacement)],
        ),
        None => invoke_func("array_join", vec![col.into(), lit(delimiter)]),
    }
}

/// Create a new map column.
pub fn create_map<I>(cols: I) -> Column
where
    I: IntoIterator,
    I::Item: Into<Column>,
{
    invoke_func("map", cols)
}

gen_func!(slice, [x: Column, start: Column, length: Column], "Returns an array containing all the elements in x from index start (array indices start at 1, or from the end if start is negative) with the specified length.");
gen_func!(concat, [cols: _], "Concatenates multiple input columns together into a single column.");

gen_func!(array_position, [col: Column, value: Column], "Locates the position of the first occurrence of the given value in the given array.");
gen_func!(element_at, [col: Column, extraction: Column], "Returns element of array at given index in extraction if col is array.");
gen_func!(array_append, [col: Column, value: Column], "Returns an array of the elements in col1 along with the added element in col2 at the last of the array.");
gen_func!(array_size, [col: Column], "Returns the total number of elements in the array.");

#[allow(unused_variables)]
pub fn array_sort(col: impl Into<Column>, compactor: Option<impl Into<Column>>) -> Column {
    unimplemented!()
}

/// adds an item into a given array at a specified array index.
pub fn array_insert(
    col: impl Into<Column>,
    pos: impl Into<Column>,
    value: impl Into<Column>,
) -> Column {
    invoke_func("array_insert", vec![col.into(), pos.into(), value.into()])
}

gen_func!(array_remove, [col: Column, element: Column], "Remove all elements that equal to element from the given array.");
gen_func!(array_prepend, [col: Column, value: Column], "Returns an array containing element as well as all elements from array.");
gen_func!(array_distinct, [col: Column], "Removes duplicate values from the array.");
gen_func!(array_intersect, [col1: Column, col2: Column], "Returns an array of the elements in the intersection of col1 and col2, without duplicates.");
gen_func!(array_union, [col1: Column, col2: Column], "Returns an array of the elements in the union of col1 and col2, without duplicates.");
gen_func!(array_except, [col1: Column, col2: Column], "Returns an array of the elements in col1 but not in col2, without duplicates.");
gen_func!(array_compact, [col: Column], "Removes null values from the array.");
gen_func!(map_from_arrays, [col1: Column, col2: Column], "Creates a new map from two arrays.");

gen_func!(explode, [col: Column], "Returns a new row for each element in the given array or map.");
gen_func!(explode_outer, [col: Column], "Returns a new row for each element in the given array or map.");
gen_func!(posexplode, [col: Column], "Returns a new row for each element with position in the given array or map.");
gen_func!(posexplode_outer, [col: Column], "Returns a new row for each element with position in the given array or map.");
gen_func!(inline, [col: Column], "Explodes an array of structs into a table.");
gen_func!(inline_outer, [col: Column], "
Explodes an array of structs into a table.");
gen_func!(get, [col: Column, index: Column], "Returns element of array at given (0-based) index.");
gen_func!(get_json_object, [col: Column, path: Column], "Extracts json object from a json string based on json path specified, and returns json string of the extracted json object.");

/// Creates a new row for a json column according to the given field names.
pub fn json_tuple<I>(col: impl Into<Column>, fields: I) -> Column
where
    I: IntoIterator<Item: AsRef<str>>,
{
    let mut args = vec![col.into()];

    args.extend(fields.into_iter().map(|f| lit(f.as_ref())));

    invoke_func("json_tuple", args)
}

/// Parses a column containing a JSON string to a row with the specific schema.
/// Returns null in the case of an unparseable string
pub fn from_json(
    col: impl Into<Column>,
    schema: impl Into<Column>,
    options: Option<HashMap<&str, &str>>,
) -> Column {
    match options {
        Some(opts) => invoke_func(
            "from_json",
            vec![col.into(), schema.into(), options_to_map(opts)],
        ),
        None => invoke_func("from_json", vec![col.into(), schema.into()]),
    }
}

/// Parses a JSON string and infers its schema in DDL format
pub fn schema_of_json(json: impl Into<Column>, options: Option<HashMap<&str, &str>>) -> Column {
    match options {
        Some(opts) => invoke_func("schema_of_json", vec![json.into(), options_to_map(opts)]),
        None => invoke_func("schema_of_json", vec![json.into()]),
    }
}

/// Converts a column containing a StructType into a JSON string
pub fn to_json(col: impl Into<Column>, options: Option<HashMap<&str, &str>>) -> Column {
    match options {
        Some(opts) => invoke_func("to_json", vec![col.into(), options_to_map(opts)]),
        None => invoke_func("to_json", vec![col.into()]),
    }
}

gen_func!(json_array_length, [col: Column], "Returns the number of elements in the outermost JSON array.");
gen_func!(json_object_keys, [col: Column], "Returns all the keys of the outermost JSON object as an array.");
gen_func!(size, [col: Column], "Returns the length of the array or map stored in the column.");
gen_func!(cardinality, [col: Column], "Returns the length of the array or map stored in the column.");

/// Creates a new struct column.
pub fn struct_col<I>(cols: I) -> Column
where
    I: IntoIterator,
    I::Item: Into<Column>,
{
    invoke_func("struct", cols)
}

/// Sorts the input array in ascending or descending order according to the natural ordering of the array elements.
pub fn sort_array(col: impl Into<Column>, asc: Option<bool>) -> Column {
    match asc {
        Some(asc) => invoke_func("sort_array", vec![col.into(), lit(asc)]),
        None => invoke_func("sort_array", vec![col.into(), lit(true)]),
    }
}

gen_func!(array_max, [col: Column], "Returns the maximum value of the array.");
gen_func!(array_min, [col: Column], "Returns the minimum value of the array.");
gen_func!(shuffle, [col: Column], "Generates a random permutation of the given array.");
gen_func!(reverse, [col: Column], "Returns a reversed string or an array with reverse order of elements.");
gen_func!(flatten, [col: Column], "Creates a single array from an array of arrays.");

/// Generate a sequence of integers from start to stop, incrementing by step.
pub fn sequence(
    start: impl Into<Column>,
    stop: impl Into<Column>,
    step: Option<impl Into<Column>>,
) -> Column {
    match step {
        Some(val) => invoke_func("sequence", vec![start.into(), stop.into(), val.into()]),
        None => invoke_func("sequence", vec![start.into(), stop.into()]),
    }
}

gen_func!(array_repeat, [col: Column, count: Column], "Creates an array containing a column repeated count times.");
gen_func!(map_contains_key, [col: Column, value: Column], "Returns true if the map contains the key.");
gen_func!(map_keys, [col: Column], "Returns an unordered array containing the keys of the map.");
gen_func!(map_values, [col: Column], "Returns an unordered array containing the values of the map.");
gen_func!(map_entries, [col: Column], "Returns an unordered array of all entries in the given map.");
gen_func!(map_from_entries, [col: Column], "Converts an array of entries (key value struct types) to a map of values.");
gen_func!(arrays_zip, [cols: _], "Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.");
gen_func!(map_concat, [cols: _], "Returns the union of all the given maps.");

/// Parses a column containing a CSV string to a row with the specific schema.
/// Returns null in the case of an unparseable string
pub fn from_csv(
    col: impl Into<Column>,
    schema: impl Into<Column>,
    options: Option<HashMap<&str, &str>>,
) -> Column {
    match options {
        Some(opts) => invoke_func(
            "from_csv",
            vec![col.into(), schema.into(), options_to_map(opts)],
        ),
        None => invoke_func("from_csv", vec![col.into(), schema.into()]),
    }
}

/// Parses a CSV string and infers its schema in DDL format
pub fn schema_of_csv(csv: impl Into<Column>, options: Option<HashMap<&str, &str>>) -> Column {
    match options {
        Some(opts) => invoke_func("schema_of_csv", vec![csv.into(), options_to_map(opts)]),
        None => invoke_func("schema_of_csv", vec![csv.into()]),
    }
}

/// Create a map after splitting the text into key/value pairs using delimiters
pub fn str_to_map(
    text: impl Into<Column>,
    pair_delim: Option<impl Into<Column>>,
    key_value_delim: Option<impl Into<Column>>,
) -> Column {
    let pair_delim = pair_delim.map(Into::into).unwrap_or_else(|| lit(","));
    let key_value_delim = key_value_delim.map(Into::into).unwrap_or_else(|| lit(":"));

    invoke_func("str_to_map", vec![text.into(), pair_delim, key_value_delim])
}

/// Converts a column containing a StructType into a CSV string
pub fn to_csv(col: impl Into<Column>, options: Option<HashMap<&str, &str>>) -> Column {
    match options {
        Some(opts) => invoke_func("to_csv", vec![col.into(), options_to_map(opts)]),
        None => invoke_func("to_csv", vec![col.into()]),
    }
}

gen_func!(try_element_at, [col: Column, extraction: Column], "Returns element of array at given (1-based) index.");

// Partition Transformations
gen_func!(years, [col: Column], "A transform for timestamps and dates to partition data into years.");
gen_func!(months, [col: Column], "A transform for timestamps and dates to partition data into months.");
gen_func!(days, [col: Column], "A transform for timestamps and dates to partition data into days.");
gen_func!(hours, [col: Column], "A transform for timestamps to partition data into hours.");
gen_func!(bucket, [num_bucket: Column, col: Column], "A transform for any type that partitions by a hash of the input column.");

// Aggregate Functions

/// Returns some value of col for a group of rows.
pub fn any_value(col: impl Into<Column>, ignore_nulls: Option<impl Into<Column>>) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func("any_value", vec![col.into(), val.into()]),
        None => invoke_func("any_value", vec![col.into(), lit(true)]),
    }
}

/// Returns a new Column for approximate distinct count of column col.
pub fn approx_count_distinct(col: impl Into<Column>, rsd: Option<f32>) -> Column {
    match rsd {
        Some(rsd) => invoke_func("approx_count_distinct", vec![col.into(), lit(rsd)]),
        None => invoke_func("approx_count_distinct", vec![col.into()]),
    }
}

gen_func!(array_agg, [col: Column], "Returns a list of objects with duplicates.");

gen_func!(avg, [col: Column], "Returns the average of the values in a group.");
gen_func!(bit_and, [col: Column], "Returns the bitwise AND of all non-null input values, or null if none.");
gen_func!(bit_or, [col: Column], "Returns the bitwise OR of all non-null input values, or null if none.");
gen_func!(bit_xor, [col: Column], "Returns the bitwise XOR of all non-null input values, or null if none.");
gen_func!(bool_and, [col: Column], "Returns true if all values of col are true.");
gen_func!(bool_or, [col: Column], "Returns true if at least one value of col is true.");
gen_func!(collect_set, [col: Column], "Returns a set of objects with duplicate elements eliminated.");
gen_func!(collect_list, [col: Column], "Returns a list of objects with duplicates.");

gen_func!(corr, [col1: Column, col2: Column], "Returns a new Column for the Pearson Correlation Coefficient for col1 and col2.");

gen_func!(count, [col: Column], "Returns the number of items in a group.");

// Returns a new Column for distinct count of col or cols
pub fn count_distinct<I>(col: impl Into<Column>, cols: Option<I>) -> Column
where
    I: IntoIterator,
    I::Item: Into<Column>,
{
    let mut cols = match cols {
        Some(val) => VecExpression::from_iter(val).expr,
        None => vec![],
    };

    let mut expr = vec![col.into().expression];

    expr.append(&mut cols);

    Column::from(spark::Expression {
        expr_type: Some(spark::expression::ExprType::UnresolvedFunction(
            spark::expression::UnresolvedFunction {
                function_name: "count".to_string(),
                arguments: VecExpression::from_iter(expr).into(),
                is_distinct: true,
                is_user_defined_function: false,
            },
        )),
    })
}

/// Returns a count-min sketch of a column with the given esp, confidence and seed.
pub fn count_min_sketch(
    col: impl Into<Column>,
    eps: impl Into<Column>,
    confidence: impl Into<Column>,
    seed: impl Into<Column>,
) -> Column {
    invoke_func(
        "count_min_sketch",
        vec![col.into(), eps.into(), confidence.into(), seed.into()],
    )
}

gen_func!(count_if, [col: Column], "Returns the number of TRUE values for the col.");
gen_func!(covar_pop, [col1: Column, col2: Column], "Returns a new Column for the population covariance of col1 and col2.");
gen_func!(covar_samp, [col1: Column, col2: Column], "Returns a new Column for the sample covariance of col1 and col2.");
gen_func!(every, [col: Column], "Returns true if all values of col are true.");

/// Returns the first value in a group.
pub fn first(col: impl Into<Column>, ignore_nulls: Option<impl Into<Column>>) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func("first", vec![col.into(), val.into()]),
        None => invoke_func("first", vec![col.into(), lit(false)]),
    }
}

/// Returns the first value of col for a group of rows.
pub fn first_value(col: impl Into<Column>, ignore_nulls: Option<impl Into<Column>>) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func("first_value", vec![col.into(), val.into()]),
        None => invoke_func("first_value", vec![col.into()]),
    }
}

gen_func!(grouping, [col: Column], "Indicates whether a specified column in a GROUP BY list is aggregated or not, returns 1 for aggregated or 0 for not aggregated in the result set.");
gen_func!(grouping_id, [cols: _], "Returns the level of grouping, equals to");
gen_func!(histogram_numeric, [col1: Column, n_bins: Column], "Computes a histogram on numeric ‘col’ using nb bins.");

/// Returns the updatable binary representation of the Datasketches HllSketch configured with lgConfigK arg.
pub fn hll_sketch_agg(col: impl Into<Column>, lg_config_k: Option<impl Into<Column>>) -> Column {
    match lg_config_k {
        Some(val) => invoke_func("hll_sketch_agg", vec![col.into(), val.into()]),
        None => invoke_func("hll_sketch_agg", vec![col.into()]),
    }
}

/// Returns the updatable binary representation of the Datasketches HllSketch, generated by merging previously created Datasketches HllSketch instances via a Datasketches Union instance.
pub fn hll_union_agg(
    col: impl Into<Column>,
    allow_different_lg_config_k: Option<impl Into<Column>>,
) -> Column {
    match allow_different_lg_config_k {
        Some(val) => invoke_func("hll_union_agg", vec![col.into(), val.into()]),
        None => invoke_func("hll_union_agg", vec![col.into()]),
    }
}

gen_func!(kurtosis, [col: Column], "Returns the kurtosis of the values in a group.");

/// Returns the last value in a group.
pub fn last(col: impl Into<Column>, ignore_nulls: Option<impl Into<Column>>) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func("last", vec![col.into(), val.into()]),
        None => invoke_func("last", vec![col.into(), lit(false)]),
    }
}

/// Returns the last value of col for a group of rows.
pub fn last_value(col: impl Into<Column>, ignore_nulls: Option<impl Into<Column>>) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func("last_value", vec![col.into(), val.into()]),
        None => invoke_func("last_value", vec![col.into()]),
    }
}

gen_func!(max, [col: Column], "Returns the maximum value of the expression in a group.");
gen_func!(max_by, [col: Column, ord: Column], "Returns the value associated with the maximum value of ord.");

/// returns the average of the values in a group.
pub fn mean(col: Column) -> Column {
    avg(col)
}

gen_func!(median, [col: Column], "Returns the median of the values in a group");
gen_func!(min, [col: Column], "Returns the minimum value of the expression in a group.");
gen_func!(min_by, [col: Column, ord: Column], "Returns the value associated with the minimum value of ord.");
gen_func!(mode, [col: Column], "Returns the most frequent value in a group.");

/// Returns the exact percentile(s) of numeric column expr at the given percentage(s) with value range in [0.0, 1.0].
pub fn percentile(
    col: impl Into<Column>,
    percentage: impl Into<Column>,
    frequency: Option<impl Into<Column>>,
) -> Column {
    match frequency {
        Some(val) => invoke_func(
            "percentile",
            vec![col.into(), percentage.into(), val.into()],
        ),
        None => invoke_func("percentile", vec![col.into(), percentage.into(), lit(1)]),
    }
}

/// Returns the approximate percentile of the numeric column col which is the smallest value in the ordered col values (sorted from least to greatest) such that no more than percentage of col values is less than the value or equal to that value.
pub fn percentile_approx(
    col: impl Into<Column>,
    percentage: impl Into<Column>,
    accuracy: Option<impl Into<Column>>,
) -> Column {
    match accuracy {
        Some(val) => invoke_func(
            "percentile",
            vec![col.into(), percentage.into(), val.into()],
        ),
        None => invoke_func(
            "percentile",
            vec![col.into(), percentage.into(), lit(10000)],
        ),
    }
}

gen_func!(product, [col: Column], "Returns the product of the values in a group.");
gen_func!(regr_avgx, [y: Column, x: Column], "Returns the average of the independent variable for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_avgy, [y: Column, x: Column], "Returns the average of the dependent variable for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_count, [y: Column, x: Column], "Returns the number of non-null number pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_intercept, [y: Column, x: Column], "Returns the intercept of the univariate linear regression line for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_r2, [y: Column, x: Column], "Returns the coefficient of determination for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_slope, [y: Column, x: Column], "Returns the slope of the linear regression line for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_sxx, [y: Column, x: Column], "Returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_sxy, [y: Column, x: Column], "Returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(regr_syy, [y: Column, x: Column], "Returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where y is the dependent variable and x is the independent variable.");
gen_func!(skewness, [col: Column], "Returns the skewness of the values in a group.");
gen_func!(some, [col: Column], "Returns true if at least one value of col is true.");
gen_func!(std, [col: Column], "Alias for stddev_samp.");
gen_func!(stddev, [col: Column], "Alias for stddev_samp.");
gen_func!(stddev_pop, [col: Column], "Returns population standard deviation of the expression in a group.");
gen_func!(stddev_samp, [col: Column], "Returns the unbiased sample standard deviation of the expression in a group.");
gen_func!(sum, [col: Column], "Returns the sum of all values in the expression.");
gen_func!(sum_distinct, [col: Column], "Returns the sum of distinct values in the expression.");
gen_func!(var_pop, [col: Column], "Returns the population variance of the values in a group.");
gen_func!(var_samp, [col: Column], "Returns the unbiased sample variance of the values in a group.");
gen_func!(variance, [col: Column], "Alias for var_samp");

// window functions

gen_func!(
    cume_dist,
    [],
    "Returns the cumulative distribution of values within a window partition, i.e."
);
gen_func!(
    dense_rank,
    [],
    "Returns the rank of rows within a window partition, without any gaps"
);

/// Returns the value that os offset rows before the current row, and default is there is less
/// than offset rows before the current row
pub fn lag(
    col: impl Into<Column>,
    offset: Option<impl Into<Column>>,
    default: Option<impl Into<Column>>,
) -> Column {
    let offset = offset.map(Into::into).unwrap_or_else(|| lit(1));

    match default {
        Some(val) => invoke_func("lag", vec![col.into(), offset, val.into()]),
        None => invoke_func("lag", vec![col.into(), offset]),
    }
}

/// Returns the value that os offset rows after the current row, and default is there is less
/// than offset rows after the current row
pub fn lead(
    col: impl Into<Column>,
    offset: Option<impl Into<Column>>,
    default: Option<impl Into<Column>>,
) -> Column {
    let offset = offset.map(Into::into).unwrap_or_else(|| lit(1));

    match default {
        Some(val) => invoke_func("lead", vec![col.into(), offset, val.into()]),
        None => invoke_func("lead", vec![col.into(), offset]),
    }
}

pub fn nth_value(
    col: impl Into<Column>,
    offset: impl Into<Column>,
    ignore_nulls: Option<bool>,
) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func("nth_value", vec![col.into(), offset.into(), lit(val)]),
        None => invoke_func("nth_value", vec![col.into(), offset.into()]),
    }
}

/// Returns the ntile group id (from 1 to n inclusive) in an ordered window partition.
pub fn ntile(n: i32) -> Column {
    invoke_func("ntitle", vec![lit(n)])
}

gen_func!(percent_rank, [], "Returns the relative rank");
gen_func!(
    rank,
    [],
    "Returns the rank of rows within a window partition."
);
gen_func!(
    row_number,
    [],
    "Returns a sequential number starting at 1 within a window partition."
);

// sort functions
/// Returns a sort expression based on the ascending order of the given column name.
pub fn asc(col: impl Into<Column>) -> Column {
    col.into().asc()
}

/// Returns a sort expression based on the ascending order of the given column name, and null values return before non-null values.
pub fn asc_nulls_first(col: impl Into<Column>) -> Column {
    col.into().asc_nulls_first()
}

/// Returns a sort expression based on the ascending order of the given column name, and null values appear after non-null values.
pub fn asc_nulls_last(col: impl Into<Column>) -> Column {
    col.into().asc_nulls_last()
}

/// Returns a sort expression based on the descending order of the given column name.
pub fn desc(col: impl Into<Column>) -> Column {
    col.into().desc()
}

/// Returns a sort expression based on the descending order of the given column name, and null values appear before non-null values.
pub fn desc_nulls_first(col: impl Into<Column>) -> Column {
    col.into().desc_nulls_first()
}

/// Returns a sort expression based on the descending order of the given column name, and null values appear after non-null values.
pub fn desc_nulls_last(col: impl Into<Column>) -> Column {
    col.into().desc_nulls_last()
}

// string functions

gen_func!(ascii, [col: Column], "Computes the numeric value of the first character of the string column.");
gen_func!(base64, [col: Column], "Computes the BASE64 encoding of a binary column and returns it as a string column.");
gen_func!(bit_length, [col: Column], "Calculates the bit length for the specified string column.");

// Remove the leading and trailing *trim* characters from *str*
pub fn btrim(str: impl Into<Column>, trim: Option<impl Into<Column>>) -> Column {
    match trim {
        Some(val) => invoke_func("btrim", vec![str.into(), val.into()]),
        None => invoke_func("btrim", vec![str.into()]),
    }
}

gen_func!(char, [col: Column], "Returns the ASCII character having the binary equivalent to col.");
gen_func!(character_length, [str: Column], "Returns the character length of string data or number of bytes of binary data.");
gen_func!(char_length, [str: Column], "Returns the character length of string data or number of bytes of binary data.");

// Concatenates multiple input string columns together into a single string column, using the given
// separator
pub fn concat_ws<I>(sep: &str, cols: I) -> Column
where
    I: IntoIterator,
    I::Item: Into<Column>,
{
    let mut cols = VecExpression::from_iter(cols).expr;
    let mut expr = vec![lit(sep).expression];
    expr.append(&mut cols);

    invoke_func("concat_ws", expr)
}

gen_func!(contains, [left: Column, right: Column], "Returns a boolean.");
gen_func!(decode, [col: Column, charset: Column], "Computes the first argument into a string from a binary using the provided character set (one of ‘US-ASCII’, ‘ISO-8859-1’, ‘UTF-8’, ‘UTF-16BE’, ‘UTF-16LE’, ‘UTF-16’).");
gen_func!(elt, [cols: _], "Returns the n-th input, e.g., returns input2 when n is 2.");
gen_func!(encode, [col: Column, charset: Column], "Computes the first argument into a string from a binary using the provided character set (one of ‘US-ASCII’, ‘ISO-8859-1’, ‘UTF-8’, ‘UTF-16BE’, ‘UTF-16LE’, ‘UTF-16’).");
gen_func!(endswith, [str: Column, suffix: Column], "Returns a boolean.");
gen_func!(find_in_set, [str: Column, str_array: Column], "Returns the index (1-based) of the given string (str) in the comma-delimited list (strArray).");
gen_func!(format_number, [col: Column, d: Column], "Formats the number X to a format like ‘#,–#,–#.–’, rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.");

// Formats the arguments in printf-style and returns the result as a string column
pub fn format_string<I>(format: &str, cols: I) -> Column
where
    I: IntoIterator,
    I::Item: Into<Column>,
{
    let mut cols = VecExpression::from_iter(cols).expr;
    let mut expr = vec![lit(format).expression];
    expr.append(&mut cols);

    invoke_func("format_string", expr)
}

pub fn ilike(
    str: impl Into<Column>,
    pattern: impl Into<Column>,
    escape_char: Option<Column>,
) -> Column {
    match escape_char {
        Some(val) => invoke_func("ilike", vec![str.into(), pattern.into(), val]),
        None => invoke_func("ilike", vec![str.into(), pattern.into()]),
    }
}

gen_func!(initcap, [col: Column], "Translate the first letter of each word to upper case in the sentence.");
gen_func!(instr, [str: Column, substr: Column], "Locate the position of the first occurrence of substr column in the given string.");
gen_func!(lcase, [str: Column], "Returns str with all characters changed to lowercase.");
gen_func!(length, [col: Column], "Computes the character length of string data or number of bytes of binary data.");

pub fn like(
    str: impl Into<Column>,
    pattern: impl Into<Column>,
    escape_char: Option<Column>,
) -> Column {
    match escape_char {
        Some(val) => invoke_func("like", vec![str.into(), pattern.into(), val]),
        None => invoke_func("like", vec![str.into(), pattern.into()]),
    }
}

gen_func!(lower, [col: Column], "Converts a string expression to lower case.");
gen_func!(left, [str: Column, len: Column], "Returns the leftmost len`(`len can be string type) characters from the string str, if len is less or equal than 0 the result is an empty string.");

pub fn levenshtein(
    left: impl Into<Column>,
    right: impl Into<Column>,
    threshold: Option<i32>,
) -> Column {
    match threshold {
        Some(val) => invoke_func("levenshtein", vec![left.into(), right.into(), lit(val)]),
        None => invoke_func("levenshtein", vec![left.into(), right.into()]),
    }
}

pub fn locate(substr: impl Into<Column>, str: impl Into<Column>, pos: Option<i32>) -> Column {
    match pos {
        Some(val) => invoke_func("locate", vec![substr.into(), str.into(), lit(val)]),
        None => invoke_func("locate", vec![substr.into(), str.into(), lit(1)]),
    }
}

gen_func!(lpad, [col: Column, len: Column, pad: Column], "Left-pad the string column to width len with pad.");
gen_func!(ltrim, [col: Column], "Trim the spaces from left end for the specified string value.");

// pub fn mask(
//     col: impl Into<Column>,
//     upper_char: Option<impl Into<Column>>,
//     lower_char: Option<impl Into<Column>>,
//     digit_char: Option<impl Into<Column>>,
//     other_char: Option<impl Into<Column>>,
// ) -> Column {
//     let upper_char = upper_char.map(Into::into).unwrap_or_else(|| lit("X"));
//     let lower_char = lower_char.map(Into::into).unwrap_or_else(|| lit("x"));
//     let digit_char = digit_char.map(Into::into).unwrap_or_else(|| lit("n"));
//     let other_char = upper_char.map(Into::into).unwrap_or_else(|| lit(None));
//
//     invoke_func(
//         "mask",
//         vec![col.into(), upper_char, lower_char, digit_char, other_char],
//     )
// }

gen_func!(octet_length, [col: Column], "Calculates the byte length for the specified string column.");

pub fn parse_url(
    url: impl Into<Column>,
    part_to_extract: impl Into<Column>,
    key: Option<impl Into<Column>>,
) -> Column {
    match key {
        Some(val) => invoke_func(
            "parse_url",
            vec![url.into(), part_to_extract.into(), val.into()],
        ),
        None => invoke_func("parse_url", vec![url.into(), part_to_extract.into()]),
    }
}

pub fn position(
    substr: impl Into<Column>,
    str: impl Into<Column>,
    start: Option<impl Into<Column>>,
) -> Column {
    match start {
        Some(val) => invoke_func("position", vec![substr.into(), str.into(), val.into()]),
        None => invoke_func("position", vec![substr.into(), str.into()]),
    }
}

pub fn printf<I>(format: impl Into<Column>, cols: I) -> Column
where
    I: IntoIterator,
    I::Item: Into<Column>,
{
    let mut cols = VecExpression::from_iter(cols).expr;
    let mut expr = vec![format.into().expression];
    expr.append(&mut cols);

    invoke_func("printf", expr)
}

gen_func!(rlike, [str: Column, regexp: Column], "Returns true if str matches the Java regex regexp, or false otherwise.");
gen_func!(regexp, [str: Column, regexp: Column], "Returns true if str matches the Java regex regexp, or false otherwise.");
gen_func!(regexp_like, [str: Column, regexp: Column], "Returns true if str matches the Java regex regexp, or false otherwise.");

pub fn regexp_extract(str: impl Into<Column>, pattern: &str, idx: i32) -> Column {
    invoke_func("regexp_extract", vec![str.into(), lit(pattern), lit(idx)])
}

pub fn regexp_extract_all(
    str: impl Into<Column>,
    regexp: impl Into<Column>,
    idx: Option<impl Into<Column>>,
) -> Column {
    match idx {
        Some(val) => invoke_func(
            "regexp_extract_all",
            vec![str.into(), regexp.into(), val.into()],
        ),
        None => invoke_func("regexp_extract_all", vec![str.into(), regexp.into()]),
    }
}

pub fn regexp_instr(
    str: impl Into<Column>,
    regexp: impl Into<Column>,
    idx: Option<impl Into<Column>>,
) -> Column {
    match idx {
        Some(val) => invoke_func("regexp_instr", vec![str.into(), regexp.into(), val.into()]),
        None => invoke_func("regexp_instr", vec![str.into(), regexp.into()]),
    }
}

gen_func!(regexp_count, [str: Column, regexp: Column], "Returns a count of the number of times that the Java regex pattern regexp is matched in the string str.");

gen_func!(regexp_replace, [string: Column, pattern: Column, replacement: Column], "Replace all substrings of the specified string value that match regexp with replacement");
gen_func!(regexp_substr, [str: Column, regexp: Column], "Returns the substring that matches the Java regex regexp within the string str.");

pub fn replace(
    src: impl Into<Column>,
    search: impl Into<Column>,
    replace: Option<impl Into<Column>>,
) -> Column {
    match replace {
        Some(val) => invoke_func("replace", vec![src.into(), search.into(), val.into()]),
        None => invoke_func("replace", vec![src.into(), search.into()]),
    }
}

gen_func!(right, [str: Column, len: Column], "Returns the rightmost len`(`len can be string type) characters from the string str, if len is less or equal than 0 the result is an empty string.");
gen_func!(ucase, [str: Column], "Returns str with all characters changed to uppercase.");
gen_func!(unbase64, [col: Column], "Decodes a BASE64 encoded string column and returns it as a binary column.");
gen_func!(rpad, [col: Column, len: Column, pad: Column], "Right-pad the string column to width len with pad.");
gen_func!(repeat, [col: Column, n: Column], "Repeats a string column n times, and returns it as a new string column.");
gen_func!(rtrim, [col: Column], "Trim the spaces from right end for the specified string value.");
gen_func!(soundex, [col: Column], "Returns the SoundEx encoding for a string");

/// Splits str around matches of the given pattern.
pub fn split(str: impl Into<Column>, pattern: &str, limit: Option<i32>) -> Column {
    let values = vec![str.into(), lit(pattern), lit(limit.unwrap_or(-1)).clone()];
    invoke_func("split", values)
}

gen_func!(split_part, [src: Column, delimiter: Column, part_num: Column], "Splits str by delimiter and return requested part of the split (1-based).");
gen_func!(startswith, [str: Column, prefix: Column], "Returns a boolean.");

pub fn substr(
    str: impl Into<Column>,
    pos: impl Into<Column>,
    len: Option<impl Into<Column>>,
) -> Column {
    match len {
        Some(val) => invoke_func("substr", vec![str.into(), pos.into(), val.into()]),
        None => invoke_func("substr", vec![str.into(), pos.into()]),
    }
}

gen_func!(substring, [src: Column, pos: Column, len: Column], "Substring starts at pos and is of length len when str is String type or returns the slice of byte array that starts at pos in byte and is of length len when str is Binary type.");
gen_func!(substring_index, [src: Column, delim: Column, count: Column], "Returns the substring from string str before count occurrences of the delimiter delim.");

pub fn overlay(
    src: impl Into<Column>,
    replace: impl Into<Column>,
    pos: impl Into<Column>,
    ignore_nulls: Option<impl Into<Column>>,
) -> Column {
    match ignore_nulls {
        Some(val) => invoke_func(
            "overlay",
            vec![src.into(), replace.into(), pos.into(), val.into()],
        ),
        None => invoke_func(
            "overlay",
            vec![src.into(), replace.into(), pos.into(), lit(-1)],
        ),
    }
}

/// Splits a string into arrays of sentences, where each sentence is an array of words
pub fn sentences(
    string: impl Into<Column>,
    language: Option<impl Into<Column>>,
    country: Option<impl Into<Column>>,
) -> Column {
    let language = language.map(Into::into).unwrap_or_else(|| lit(""));
    let country = country.map(Into::into).unwrap_or_else(|| lit(""));

    invoke_func("sentences", vec![string.into(), language, country])
}

pub fn to_binary(col: impl Into<Column>, len: Option<impl Into<Column>>) -> Column {
    match len {
        Some(val) => invoke_func("to_binary", vec![col.into(), val.into()]),
        None => invoke_func("to_binary", vec![col.into()]),
    }
}

pub fn try_to_binary(col: impl Into<Column>, len: Option<impl Into<Column>>) -> Column {
    match len {
        Some(val) => invoke_func("try_to_binary", vec![col.into(), val.into()]),
        None => invoke_func("try_to_binary", vec![col.into()]),
    }
}

gen_func!(to_char, [col: Column, format: Column], "Convert col to a string based on the format.");
gen_func!(to_number, [col: Column, format: Column], "Convert string ‘col’ to a number based on the string format ‘format’");
gen_func!(try_to_number, [col: Column, format: Column], "Convert string ‘col’ to a number based on the string format ‘format’");
gen_func!(to_varchar, [col: Column, format: Column], "Convert col to a string based on the format.");
gen_func!(translate, [src_col: Column, matching: Column, replace: Column], "A function translate any character in the srcCol by a character in matching.");
gen_func!(trim, [col: Column], "Trim the spaces from both ends for the specified string column.");
gen_func!(upper, [col: Column], "Converts a string expression to upper case.");
gen_func!(url_decode, [str: Column], "Decodes a str in ‘application/x-www-form-urlencoded’ format using a specific encoding scheme.");
gen_func!(url_encode, [str: Column], "Translates a string into ‘application/x-www-form-urlencoded’ format using a specific encoding scheme.");

// bitwise functions

gen_func!(bit_count, [col: Column], "Returns the number of bits that are set in the argument expr as an unsigned 64-bit integer, or NULL if the argument is NULL.");
gen_func!(bit_get, [col: Column, pos: Column], "Returns the value of the bit (0 or 1) at the specified position.");
gen_func!(getbit, [col: Column, pos: Column], "Returns the value of the bit (0 or 1) at the specified position.");

// misc functions

/// Returns a decrypted value of *input* using AES in *mode* with *padding*
pub fn aes_decrypt(
    input: impl Into<Column>,
    key: impl Into<Column>,
    mode: Option<impl Into<Column>>,
    padding: Option<impl Into<Column>>,
    aad: Option<impl Into<Column>>,
) -> Column {
    let mode = mode.map(Into::into).unwrap_or_else(|| lit("GCM"));
    let padding = padding.map(Into::into).unwrap_or_else(|| lit("DEFAULT"));
    let aad = aad.map(Into::into).unwrap_or_else(|| lit(""));

    invoke_func(
        "aes_decrypt",
        vec![input.into(), key.into(), mode, padding, aad],
    )
}

pub fn try_aes_decrypt(
    input: impl Into<Column>,
    key: impl Into<Column>,
    mode: Option<impl Into<Column>>,
    padding: Option<impl Into<Column>>,
    aad: Option<impl Into<Column>>,
) -> Column {
    let mode = mode.map(Into::into).unwrap_or_else(|| lit("GCM"));
    let padding = padding.map(Into::into).unwrap_or_else(|| lit("DEFAULT"));
    let aad = aad.map(Into::into).unwrap_or_else(|| lit(""));

    invoke_func(
        "try_aes_decrypt",
        vec![input.into(), key.into(), mode, padding, aad],
    )
}

/// Returns a encrypted value of *input* using AES in *mode* with the specified *padding*
pub fn aes_encrypt(
    input: impl Into<Column>,
    key: impl Into<Column>,
    mode: Option<impl Into<Column>>,
    padding: Option<impl Into<Column>>,
    iv: Option<impl Into<Column>>,
    aad: Option<impl Into<Column>>,
) -> Column {
    let mode = mode.map(Into::into).unwrap_or_else(|| lit("GCM"));
    let padding = padding.map(Into::into).unwrap_or_else(|| lit("DEFAULT"));
    let iv = iv.map(Into::into).unwrap_or_else(|| lit(""));
    let aad = aad.map(Into::into).unwrap_or_else(|| lit(""));

    invoke_func(
        "aes_encrypt",
        vec![input.into(), key.into(), mode, padding, iv, aad],
    )
}

gen_func!(bitmap_bit_position, [col: Column], "Returns the bit position for the given input column.");
gen_func!(bitmap_bucket_number, [col: Column], "Returns the bucket number for the given input column.");
gen_func!(bitmap_construct_agg, [col: Column], "Returns a bitmap with the positions of the bits set from all the values from the input column.");
gen_func!(bitmap_count, [col: Column], "Returns the number of set bits in the input bitmap.");
gen_func!(bitmap_or_agg, [col: Column], "Returns a bitmap that is the bitwise OR of all of the bitmaps from the input column.");
gen_func!(current_catalog, [], "Returns the current catalog.");
gen_func!(current_database, [], "Returns the current database.");
gen_func!(current_schema, [], "Returns the current database.");
gen_func!(current_user, [], "Returns the current user.");
gen_func!(
    input_file_block_length,
    [],
    "Returns the length of the block being read, or -1 if not available."
);
gen_func!(
    input_file_block_start,
    [],
    "Returns the start offset of the block being read, or -1 if not available."
);
gen_func!(md5, [col: Column], "Calculates the MD5 digest and returns the value as a 32 character hex string.");
gen_func!(sha, [col: Column], "Returns a sha1 hash value as a hex string of the col.");
gen_func!(sha1, [col: Column], "Returns the hex string result of SHA-1.");
gen_func!(sha2, [col: Column, num_bits: Column], "Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512).");
gen_func!(crc32, [col: Column], "Calculates the cyclic redundancy check value (CRC32) of a binary column and returns the value as a bigint.");
gen_func!(hash, [cols: _], "Calculates the hash code of given columns, and returns the result as an int column.");
gen_func!(xxhash64, [cols: _], "Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column.");

/// Returns *null* if the input column is *true*; throws an exception with the provided error
/// message otherwise
pub fn assert_true(col: impl Into<Column>, err_msg: Option<impl Into<Column>>) -> Column {
    match err_msg {
        Some(val) => invoke_func("assert_true", vec![col.into(), val.into()]),
        None => invoke_func("assert_true", vec![col.into()]),
    }
}

gen_func!(raise_error, [col: Column], "Throws an exception with the provided error message.");
gen_func!(reflect, [cols: _], "Calls a method with reflection.");
gen_func!(hll_sketch_estimate, [col: Column], "Returns the estimated number of unique values given the binary representation of a Datasketches HllSketch.");

pub fn hll_union(
    col1: impl Into<Column>,
    col2: impl Into<Column>,
    allow_different_lg_config_k: Option<bool>,
) -> Column {
    match allow_different_lg_config_k {
        Some(val) => invoke_func("hll_union", vec![col1.into(), col2.into(), lit(val)]),
        None => invoke_func("hll_union", vec![col1.into(), col2.into()]),
    }
}

gen_func!(java_method, [cols: _], "Calls a method with reflection.");
gen_func!(stack, [cols: _], "Separates col1, …, colk into n rows");
gen_func!(user, [], "Returns the current database.");
gen_func!(version, [], "Returns the Spark version.");

// predicate functions
gen_func!(equal_null, [col1: Column, col2: Column], "Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.");
gen_func!(ifnull, [col1: Column, col2: Column], "Returns col2 if col1 is null, or col1 otherwise.");
gen_func!(isnotnull, [col: Column], "Returns true if col is not null, or false otherwise.");
gen_func!(nullif, [col1: Column, col2: Column], "Returns null if col1 equals to col2, or col1 otherwise.");
gen_func!(nvl, [col1: Column, col2: Column], "Returns col2 if col1 is null, or col1 otherwise.");
gen_func!(nvl2, [col1: Column, col2: Column, col3: Column], "Returns col2 if col1 is not null, or col3 otherwise.");

// xml functions
gen_func!(xpath, [xml: Column, path: Column], "Returns a string array of values within the nodes of xml that match the XPath expression.");
gen_func!(xpath_boolean, [xml: Column, path: Column], "Returns true if the XPath expression evaluates to true, or if a matching node is found.");
gen_func!(xpath_double, [xml: Column, path: Column], "Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.");
gen_func!(xpath_float, [xml: Column, path: Column], "Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.");
gen_func!(xpath_int, [xml: Column, path: Column], "Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.");
gen_func!(xpath_long, [xml: Column, path: Column], "Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.");
gen_func!(xpath_number, [xml: Column, path: Column], "Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.");
gen_func!(xpath_short, [xml: Column, path: Column], "Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.");
gen_func!(xpath_string, [xml: Column, path: Column], "Returns the text contents of the first xml node that matches the XPath expression.");

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

    macro_rules! test_func {
        ($func_name:ident, $sql_stmt:expr, $func_expr:expr, $col_name:ident, $arrow:expr, $nullable:literal) => {
            #[tokio::test]
            async fn $func_name() -> Result<(), SparkError> {
                let spark = setup().await;

                let df = spark.sql($sql_stmt).await?;

                let res = df.select([$func_expr]).collect().await?;

                let $col_name: ArrayRef = Arc::new($arrow);

                let expected = RecordBatch::try_from_iter_with_nullable(vec![(
                    stringify!($col_name),
                    $col_name,
                    $nullable,
                )])?;

                assert_eq!(expected, res);
                Ok(())
            }
        };
    }

    // normal functions
    test_func!(
        test_func_cast,
        "SELECT 1 AS age",
        col("age").cast("string"),
        age,
        StringArray::from(vec!["1"]),
        false
    );

    test_func!(
        test_func_alias,
        "SELECT 1 AS age",
        col("age").alias("age_new"),
        age_new,
        Int32Array::from(vec![1]),
        false
    );

    test_func!(
        test_func_lit_int,
        "SELECT 1 AS id",
        lit(5).alias("value"),
        value,
        Int32Array::from(vec![5]),
        false
    );

    test_func!(
        test_func_lit_str,
        "SELECT 1 AS id",
        lit("hello").alias("value"),
        value,
        StringArray::from(vec!["hello"]),
        false
    );

    test_func!(
        test_func_coalesce,
        "SELECT 1 as col1, null as col2 UNION SELECT null, 2",
        coalesce([col("col1"), col("col2")]).alias("value"),
        value,
        Int32Array::from(vec![1, 2]),
        true
    );

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

    test_func!(
        test_func_isnull,
        "SELECT 1 as col1 UNION SELECT null",
        isnull("col1").alias("value"),
        value,
        BooleanArray::from(vec![false, true]),
        false
    );

    test_func!(
        test_func_monotonically_id,
        "SELECT explode(sequence(1, 5)) as id",
        monotonically_increasing_id().alias("value"),
        value,
        Int64Array::from(vec![0, 1, 2, 3, 4]),
        false
    );

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

    test_func!(
        test_func_bitwise_not,
        "SELECT explode(sequence(0, 1)) as id",
        bitwise_not("id").alias("value"),
        value,
        Int32Array::from(vec![-1, -2]),
        false
    );

    test_func!(
        test_func_expr,
        "SELECT 'Alice' AS name UNION SELECT 'Bob'",
        expr("length(name)").alias("value"),
        value,
        Int32Array::from(vec![5, 3]),
        false
    );

    test_func!(
        test_func_greatest,
        "SELECT 1 as a, 4 as b, 4 as c",
        greatest(["a", "b", "c"]).alias("value"),
        value,
        Int32Array::from(vec![4]),
        false
    );

    test_func!(
        test_func_least,
        "SELECT 1 as a, 4 as b, 4 as c",
        least(["a", "b", "c"]).alias("value"),
        value,
        Int32Array::from(vec![1]),
        false
    );

    // math functions
    test_func!(
        test_func_sqrt,
        "SELECT 4 as id",
        sqrt("id").alias("value"),
        value,
        Float64Array::from(vec![2.0]),
        true
    );

    // column operators
    test_func!(
        test_func_add,
        "SELECT explode(sequence(1,4)) as val",
        (col("val") + lit(4)).alias("value"),
        value,
        Int32Array::from(vec![5, 6, 7, 8]),
        false
    );

    test_func!(
        test_func_subtract,
        "SELECT explode(sequence(1,4)) as val",
        (col("val") - lit(1)).alias("value"),
        value,
        Int32Array::from(vec![0, 1, 2, 3]),
        false
    );

    test_func!(
        test_func_multiple,
        "SELECT explode(sequence(1,4)) as val",
        (col("val") * lit(2)).alias("value"),
        value,
        Int32Array::from(vec![2, 4, 6, 8]),
        false
    );

    // Test sort functions and column methods
    #[tokio::test]
    async fn test_func_from_csv() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark.sql("SELECT '     abc' as value").await?;

        let mut opts = HashMap::new();
        opts.insert("ignoreLeadingWhiteSpace", "True");

        let res = df
            .select([from_csv("value", lit("a STRING"), Some(opts)).alias("value")])
            .collect()
            .await?;

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![2]));
        let c: ArrayRef = Arc::new(Int32Array::from(vec![3]));

        let expected = RecordBatch::try_from_iter_with_nullable(vec![
            ("a", a, false),
            ("b", b, false),
            ("c", c, false),
        ])?;

        assert_eq!(expected, res);
        Ok(())
    }

    // Test sort functions and column methods
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
