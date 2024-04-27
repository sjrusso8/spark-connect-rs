//! [Column] represents a column in a DataFrame that holds a [spark::Expression]
use std::convert::From;
use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Not, Rem, Sub};

use crate::spark;

use crate::expressions::{ToExpr, ToLiteralExpr};
use crate::functions::invoke_func;
use crate::window::WindowSpec;

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

impl From<spark::Expression> for Column {
    /// Used for creating columns from a [spark::Expression]
    fn from(expression: spark::Expression) -> Self {
        Self { expression }
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
                        plan_id: Some(1),
                    },
                )),
            },
        };

        Column::from(expression)
    }
}

impl Column {
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

    /// Casts the column into the Spark type represented as a `&str`
    ///
    /// # Arguments:
    ///
    /// * `to_type` is the string representation of the datatype
    ///
    /// # Example:
    /// ```rust
    /// let df = df.select([
    ///       col("age").cast("int"),
    ///       col("name").cast("string")
    ///     ])
    /// ```
    pub fn cast(self, to_type: &str) -> Column {
        let type_str = spark::expression::cast::CastToType::TypeStr(to_type.to_string());

        let cast = spark::expression::Cast {
            expr: Some(Box::new(self.expression)),
            cast_to_type: Some(type_str),
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
    pub fn isin<T: ToLiteralExpr>(self, cols: Vec<T>) -> Column {
        let mut values = cols
            .iter()
            .map(|col| Column::from(col.to_literal_expr()))
            .collect::<Vec<Column>>();

        values.insert(0, self);

        invoke_func("in", values)
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
    pub fn contains<T: ToLiteralExpr>(self, other: T) -> Column {
        invoke_func("contains", vec![self.to_expr(), other.to_literal_expr()])
    }

    /// A filter expression that evaluates if the column startswith a string literal
    pub fn startswith<T: ToLiteralExpr>(self, other: T) -> Column {
        invoke_func("startswith", vec![self.to_expr(), other.to_literal_expr()])
    }

    /// A filter expression that evaluates if the column endswith a string literal
    pub fn endswith<T: ToLiteralExpr>(self, other: T) -> Column {
        invoke_func("endswith", vec![self.to_expr(), other.to_literal_expr()])
    }

    /// A SQL LIKE filter expression that evaluates the column based on a case sensitive match
    pub fn like<T: ToLiteralExpr>(self, other: T) -> Column {
        invoke_func("like", vec![self.to_expr(), other.to_literal_expr()])
    }

    /// A SQL ILIKE filter expression that evaluates the column based on a case insensitive match
    pub fn ilike<T: ToLiteralExpr>(self, other: T) -> Column {
        invoke_func("ilike", vec![self.to_expr(), other.to_literal_expr()])
    }

    /// A SQL RLIKE filter expression that evaluates the column based on a regex match
    pub fn rlike<T: ToLiteralExpr>(self, other: T) -> Column {
        invoke_func("rlike", vec![self.to_expr(), other.to_literal_expr()])
    }

    /// Equality comparion. Cannot overload the '==' and return something other
    /// than a bool
    pub fn eq<T: ToExpr>(self, other: T) -> Column {
        invoke_func("==", vec![self.to_expr(), other.to_expr()])
    }

    /// Logical AND comparion. Cannot overload the '&&' and return something other
    /// than a bool
    pub fn and<T: ToExpr>(self, other: T) -> Column {
        invoke_func("and", vec![self.to_expr(), other.to_expr()])
    }

    /// Logical OR comparion.
    pub fn or<T: ToExpr>(self, other: T) -> Column {
        invoke_func("or", vec![self.to_expr(), other.to_expr()])
    }
    /// A filter expression that evaluates to true is the expression is null
    #[allow(non_snake_case)]
    pub fn isNull(self) -> Column {
        invoke_func("isnull", self)
    }

    /// A filter expression that evaluates to true is the expression is NOT null
    #[allow(non_snake_case)]
    pub fn isNotNull(self) -> Column {
        invoke_func("isnotnull", self)
    }

    #[allow(non_snake_case)]
    pub fn isNaN(self) -> Column {
        invoke_func("isNaN", self)
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
    ///     .partitionBy(col("name"))
    ///     .orderBy([col("age")])
    ///     .rangeBetween(Window::unboundedPreceding(), Window::currentRow());
    ///
    /// let df = df.withColumn("rank", rank().over(window.clone()))
    ///     .withColumn("min", min("age").over(window));
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

impl Add for Column {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        invoke_func("+", vec![self, other])
    }
}

impl Neg for Column {
    type Output = Self;

    fn neg(self) -> Self {
        invoke_func("negative", self)
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
