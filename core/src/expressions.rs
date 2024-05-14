//! Traits for converting Rust Types to Spark Connect Expression Types
//!
//! Spark Connect has a few different ways of creating expressions and different gRPC methods
//! require expressions in different forms. These traits are used to either translate a value into
//! a [spark::Expression] or into a [spark::expression::Literal].
//!
//! ## Overview
//!
//! - [ToExpr] accepts a `&str`, `String`, or [Column]. This trait uses the method `from`
//! on the Column to create an expression.
//! - [ToLiteral] is used for taking rust types into a [spark::expression::Literal]. These values
//! are then converted into an expression
//! - [ToLiteralExpr`] takes a literal value and converts it into a [spark::Expression]
//! - [ToVecExpr] many gRPC methods require a `Vec<spark::Expression>` this trait is a shorthand
//! for that transformation
//! - [ToFilterExpr] is specifically used for filter statements
//!

use crate::spark;

use crate::column::Column;
use crate::types::ToDataType;

/// Translate string values into a `spark::Expression`
pub trait ToExpr {
    fn to_expr(&self) -> spark::Expression;
}

impl ToExpr for &str {
    fn to_expr(&self) -> spark::Expression {
        Column::from(*self).expression.clone()
    }
}

impl ToExpr for String {
    fn to_expr(&self) -> spark::Expression {
        Column::from(self.as_str()).expression.clone()
    }
}

impl ToExpr for Column {
    fn to_expr(&self) -> spark::Expression {
        self.expression.clone()
    }
}

/// Translate values into a `Vec<spark::Expression>`
pub trait ToVecExpr {
    fn to_vec_expr(&self) -> Vec<spark::Expression>;
}

impl<T> ToVecExpr for T
where
    T: ToExpr,
{
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        vec![self.to_expr()]
    }
}

impl ToVecExpr for Vec<spark::Expression> {
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        self.to_vec()
    }
}

impl<T> ToVecExpr for Vec<T>
where
    T: ToExpr,
{
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        self.iter().map(|col| col.to_expr()).collect()
    }
}

impl<const N: usize, T> ToVecExpr for [T; N]
where
    T: ToExpr,
{
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        self.iter().map(|col| col.to_expr()).collect()
    }
}

/// Create a filter expression
pub trait ToFilterExpr {
    fn to_filter_expr(&self) -> Option<spark::Expression>;
}

impl ToFilterExpr for Column {
    fn to_filter_expr(&self) -> Option<spark::Expression> {
        Some(self.to_expr())
    }
}

impl ToFilterExpr for &str {
    fn to_filter_expr(&self) -> Option<spark::Expression> {
        let expr_type = Some(spark::expression::ExprType::ExpressionString(
            spark::expression::ExpressionString {
                expression: self.to_string(),
            },
        ));

        Some(spark::Expression { expr_type })
    }
}

/// Translate a rust value into a literal type
pub trait ToLiteral {
    fn to_literal(&self) -> spark::expression::Literal;
}

macro_rules! impl_to_literal {
    ($type:ty, $inner_type:ident) => {
        impl ToLiteral for $type {
            fn to_literal(&self) -> spark::expression::Literal {
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::$inner_type(*self)),
                }
            }
        }
    };
}

impl_to_literal!(bool, Boolean);
impl_to_literal!(i32, Integer);
impl_to_literal!(i64, Long);
impl_to_literal!(f32, Float);
impl_to_literal!(f64, Double);

impl ToLiteral for &[u8] {
    fn to_literal(&self) -> spark::expression::Literal {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Binary(Vec::from(
                *self,
            ))),
        }
    }
}

impl ToLiteral for i16 {
    fn to_literal(&self) -> spark::expression::Literal {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Short(*self as i32)),
        }
    }
}

impl ToLiteral for String {
    fn to_literal(&self) -> spark::expression::Literal {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::String(
                self.clone(),
            )),
        }
    }
}

impl ToLiteral for &str {
    fn to_literal(&self) -> spark::expression::Literal {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::String(
                self.to_string(),
            )),
        }
    }
}

impl<Tz: chrono::TimeZone> ToLiteral for chrono::DateTime<Tz> {
    fn to_literal(&self) -> spark::expression::Literal {
        // timestamps for spark have to be the microsends since 1/1/1970
        let timestamp = self.timestamp_micros();

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Timestamp(
                timestamp,
            )),
        }
    }
}

impl ToLiteral for chrono::NaiveDateTime {
    fn to_literal(&self) -> spark::expression::Literal {
        // timestamps for spark have to be the microsends since 1/1/1970
        let timestamp = self.and_utc().timestamp_micros();

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::TimestampNtz(
                timestamp,
            )),
        }
    }
}

impl ToLiteral for chrono::NaiveDate {
    fn to_literal(&self) -> spark::expression::Literal {
        // Spark works based on unix time. I.e. seconds since 1/1/1970
        // to get dates to work you have to do this math
        let days_since_unix_epoch =
            self.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Date(
                days_since_unix_epoch.num_days() as i32,
            )),
        }
    }
}

/// Wrap a literal value into a `spark::Expression`
pub trait ToLiteralExpr {
    fn to_literal_expr(&self) -> spark::Expression;
}

impl<T> ToLiteralExpr for T
where
    T: ToLiteral,
{
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(self.to_literal())),
        }
    }
}

impl ToLiteralExpr for Column {
    fn to_literal_expr(&self) -> spark::Expression {
        self.to_expr()
    }
}

/// Create an Array Spark Type from a Vec
impl<T> ToLiteralExpr for Vec<T>
where
    T: ToDataType + ToLiteral,
{
    fn to_literal_expr(&self) -> spark::Expression {
        let kind = self
            .first()
            .expect("Array can not be empty")
            .to_proto_type();

        let literal_vec = self.iter().map(|val| val.to_literal()).collect();

        let array_type = spark::expression::literal::Array {
            element_type: Some(kind),
            elements: literal_vec,
        };

        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Array(array_type)),
                },
            )),
        }
    }
}

/// Create an Array Spark Type from a Slice
impl<const N: usize, T> ToLiteralExpr for [T; N]
where
    T: ToDataType + ToLiteral,
{
    fn to_literal_expr(&self) -> spark::Expression {
        let kind = self
            .first()
            .expect("Array can not be empty")
            .to_proto_type();

        let literal_vec = self.iter().map(|val| val.to_literal()).collect();

        let array_type = spark::expression::literal::Array {
            element_type: Some(kind),
            elements: literal_vec,
        };

        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Array(array_type)),
                },
            )),
        }
    }
}
