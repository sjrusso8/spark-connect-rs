//! [Column] represents a column in a DataFrame that holds a [spark::Expression]
use std::convert::From;
use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Not, Rem, Sub};

use crate::spark;

use crate::functions::invoke_func;
use crate::window::WindowSpec;

use spark::expression::cast::CastToType;

/// # Column
///
/// A column holds a specific [spark::Expression] which will be resolved once an action is called.
/// The columns are resolved by the Spark Connect server of the remote session.
///
/// A column instance can be created by in a similar way as to the Spark API. A column with created
/// with `col("*")` or `col("name.*")` is created as an unresolved star attribute which will select
/// all columns or references in the specified column.
///
/// ```rust
/// use spark_connect_rs::{SparkSession, SparkSessionBuilder};
///
/// let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=example_rs".to_string())
///         .build()
///         .await?;
///
/// // As a &str representing an unresolved column in the dataframe
/// spark.range(None, 1, 1, Some(1)).select("id");
///
/// // By using the `col` function
/// spark.range(None, 1, 1, Some(1)).select(col("id"));
///
/// // By using the `lit` function to return a literal value
/// spark.range(None, 1, 1, Some(1)).select(lit(4.0).alias("num_col"));
///
/// ```
#[derive(Clone, Debug)]
pub struct Column {
    /// a [spark::Expression] containing any unresolved value to be leveraged in a [spark::Plan]
    pub expression: spark::Expression,
}

impl Column {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        Self::from(s)
    }

    pub fn from_string(s: String) -> Self {
        Self::from(s.as_str())
    }

    /// Returns the column with a new name
    ///
    /// # Example:
    /// ```rust
    /// let cols = [
    ///     col("name").alias("new_name"),
    ///     col("age").alias("new_age")
    /// ];
    ///
    /// df.select(cols);
    /// ```
    pub fn alias(self, value: &str) -> Column {
        let alias = spark::expression::Alias {
            expr: Some(Box::new(self.expression)),
            name: vec![value.to_string()],
            metadata: None,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::Alias(Box::new(alias))),
        };

        Column::from(expression)
    }

    /// An alias for the function `alias`
    pub fn name(self, value: &str) -> Column {
        self.alias(value)
    }

    /// Returns a sorted expression based on the ascending order of the column
    ///
    /// # Example:
    /// ```rust
    /// let df: DataFrame = df.sort(col("id").asc());
    ///
    /// let df: DataFrame = df.sort(asc(col("id")));
    /// ```
    pub fn asc(self) -> Column {
        self.asc_nulls_first()
    }

    pub fn asc_nulls_first(self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression)),
            direction: 1,
            null_ordering: 1,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn asc_nulls_last(self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression)),
            direction: 1,
            null_ordering: 2,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    /// Returns a sorted expression based on the ascending order of the column
    ///
    /// # Example:
    /// ```rust
    /// let df: DataFrame = df.sort(col("id").desc());
    ///
    /// let df: DataFrame = df.sort(desc(col("id")));
    /// ```
    pub fn desc(self) -> Column {
        self.desc_nulls_first()
    }

    pub fn desc_nulls_first(self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression)),
            direction: 2,
            null_ordering: 1,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn desc_nulls_last(self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression)),
            direction: 2,
            null_ordering: 2,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn drop_fields<I, T>(self, field_names: I) -> Column
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        let mut parent_col = self.expression;

        for field in field_names {
            parent_col = spark::Expression {
                expr_type: Some(spark::expression::ExprType::UpdateFields(Box::new(
                    spark::expression::UpdateFields {
                        struct_expression: Some(Box::new(parent_col)),
                        field_name: field.as_ref().to_string(),
                        value_expression: None,
                    },
                ))),
            };
        }

        Column::from(parent_col)
    }

    pub fn with_field(self, field_name: &str, col: impl Into<Column>) -> Column {
        let update_field = spark::Expression {
            expr_type: Some(spark::expression::ExprType::UpdateFields(Box::new(
                spark::expression::UpdateFields {
                    struct_expression: Some(Box::new(self.expression)),
                    field_name: field_name.to_string(),
                    value_expression: Some(Box::new(col.into().expression)),
                },
            ))),
        };

        Column::from(update_field)
    }

    pub fn substr(self, start_pos: impl Into<Column>, length: impl Into<Column>) -> Column {
        invoke_func("substr", vec![self, start_pos.into(), length.into()])
    }

    /// Casts the column into the Spark DataType
    ///
    /// # Arguments:
    ///
    /// * `to_type` is a string or [DataType] of the target type
    ///
    /// # Example:
    /// ```rust
    /// use crate::types::DataType;
    ///
    /// let df = df.select([
    ///       col("age").cast("int"),
    ///       col("name").cast("string")
    ///     ])
    ///
    /// // Using DataTypes
    /// let df = df.select([
    ///       col("age").cast(DataType::Integer),
    ///       col("name").cast(DataType::String)
    ///     ])
    /// ```
    pub fn cast(self, to_type: impl Into<CastToType>) -> Column {
        let cast = spark::expression::Cast {
            expr: Some(Box::new(self.expression)),
            cast_to_type: Some(to_type.into()),
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::Cast(Box::new(cast))),
        };

        Column::from(expression)
    }

    /// A boolean expression that is evaluated to `true` if the value of the expression is
    /// contained by the evaluated values of the arguments
    ///
    /// # Arguments:
    ///
    /// * `cols` a value that implements the [ToLiteralExpr] trait
    ///
    /// # Example:
    /// ```rust
    /// df.filter(col("name").isin(["Jorge", "Bob"]));
    /// ```
    pub fn isin(self, cols: Vec<Column>) -> Column {
        let mut val = cols.clone();

        val.insert(0, self);

        invoke_func("in", val)
    }

    /// A boolean expression that is evaluated to `true` if the value is in the Column
    ///
    /// # Arguments:
    ///
    /// * `cols`: a col reference that is translated into an [spark::Expression]
    ///
    /// # Example:
    /// ```rust
    /// df.filter(col("name").contains("ge"));
    /// ```
    pub fn contains(self, other: impl Into<Column>) -> Column {
        invoke_func("contains", vec![self, other.into()])
    }

    /// A filter expression that evaluates if the column startswith a string literal
    pub fn startswith(self, other: impl Into<Column>) -> Column {
        invoke_func("startswith", vec![self, other.into()])
    }

    /// A filter expression that evaluates if the column endswith a string literal
    pub fn endswith(self, other: impl Into<Column>) -> Column {
        invoke_func("endswith", vec![self, other.into()])
    }

    /// A SQL LIKE filter expression that evaluates the column based on a case sensitive match
    pub fn like(self, other: impl Into<Column>) -> Column {
        invoke_func("like", vec![self, other.into()])
    }

    /// A SQL ILIKE filter expression that evaluates the column based on a case insensitive match
    pub fn ilike(self, other: impl Into<Column>) -> Column {
        invoke_func("ilike", vec![self, other.into()])
    }

    /// A SQL RLIKE filter expression that evaluates the column based on a regex match
    pub fn rlike(self, other: impl Into<Column>) -> Column {
        invoke_func("rlike", vec![self, other.into()])
    }

    /// Equality comparion. Cannot overload the '==' and return something other
    /// than a bool
    pub fn eq(self, other: impl Into<Column>) -> Column {
        invoke_func("==", vec![self, other.into()])
    }

    /// Logical AND comparion. Cannot overload the '&&' and return something other
    /// than a bool
    pub fn and(self, other: impl Into<Column>) -> Column {
        invoke_func("and", vec![self, other.into()])
    }

    /// Logical OR comparion.
    pub fn or(self, other: impl Into<Column>) -> Column {
        invoke_func("or", vec![self, other.into()])
    }

    /// A filter expression that evaluates to true is the expression is null
    pub fn is_null(self) -> Column {
        invoke_func("isnull", vec![self])
    }

    /// A filter expression that evaluates to true is the expression is NOT null
    pub fn is_not_null(self) -> Column {
        invoke_func("isnotnull", vec![self])
    }

    pub fn is_nan(self) -> Column {
        invoke_func("isNaN", vec![self])
    }

    /// Defines a windowing column
    /// # Arguments:
    ///
    /// * `window`: a [WindowSpec]
    ///
    /// # Example
    ///
    /// ```
    /// let window = Window::new()
    ///     .partition_by(col("name"))
    ///     .order_by([col("age")])
    ///     .range_between(Window::unbounded_preceding(), Window::current_row());
    ///
    /// let df = df.with_column("rank", rank().over(window.clone()))
    ///     .with_column("min", min("age").over(window));
    /// ```
    pub fn over(self, window: WindowSpec) -> Column {
        let window_expr = spark::expression::Window {
            window_function: Some(Box::new(self.expression)),
            partition_spec: window.partition_spec,
            order_spec: window.order_spec,
            frame_spec: window.frame_spec,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::Window(Box::new(window_expr))),
        };

        Column::from(expression)
    }
}

impl From<spark::Expression> for Column {
    /// Used for creating columns from a [spark::Expression]
    fn from(expression: spark::Expression) -> Self {
        Self { expression }
    }
}

impl From<spark::expression::Literal> for Column {
    /// Used for creating columns from a [spark::Expression]
    fn from(expression: spark::expression::Literal) -> Self {
        Self::from(spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(expression)),
        })
    }
}

impl From<String> for Column {
    fn from(value: String) -> Self {
        Column::from_string(value)
    }
}

impl From<&String> for Column {
    fn from(value: &String) -> Self {
        Column::from_str(value.as_str())
    }
}

impl From<&str> for Column {
    /// `&str` values containing a `*` will be created as an unresolved star expression
    /// Otherwise, the value is created as an unresolved attribute
    fn from(value: &str) -> Self {
        let expression = match value {
            "*" => spark::Expression {
                expr_type: Some(spark::expression::ExprType::UnresolvedStar(
                    spark::expression::UnresolvedStar {
                        unparsed_target: None,
                    },
                )),
            },
            value if value.ends_with(".*") => spark::Expression {
                expr_type: Some(spark::expression::ExprType::UnresolvedStar(
                    spark::expression::UnresolvedStar {
                        unparsed_target: Some(value.to_string()),
                    },
                )),
            },
            _ => spark::Expression {
                expr_type: Some(spark::expression::ExprType::UnresolvedAttribute(
                    spark::expression::UnresolvedAttribute {
                        unparsed_identifier: value.to_string(),
                        plan_id: None,
                    },
                )),
            },
        };

        Column::from(expression)
    }
}

impl Add for Column {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        invoke_func("+", vec![self, other])
    }
}

impl Neg for Column {
    type Output = Self;

    fn neg(self) -> Self {
        invoke_func("negative", vec![self])
    }
}

impl Sub for Column {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        invoke_func("-", vec![self, other])
    }
}

impl Mul for Column {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        invoke_func("*", vec![self, other])
    }
}

impl Div for Column {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        invoke_func("/", vec![self, other])
    }
}

impl Rem for Column {
    type Output = Self;

    fn rem(self, other: Self) -> Self {
        invoke_func("%", vec![self, other])
    }
}

impl BitOr for Column {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        invoke_func("|", vec![self, other])
    }
}

impl BitAnd for Column {
    type Output = Self;

    fn bitand(self, other: Self) -> Self {
        invoke_func("&", vec![self, other])
    }
}

impl BitXor for Column {
    type Output = Self;

    fn bitxor(self, other: Self) -> Self {
        invoke_func("^", vec![self, other])
    }
}

impl Not for Column {
    type Output = Self;

    fn not(self) -> Self::Output {
        invoke_func("not", vec![self])
    }
}
