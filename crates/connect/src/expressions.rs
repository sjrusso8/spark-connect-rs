//! Traits for converting Rust Types to Spark Connect Expression Types
//!
//! Spark Connect has a few different ways of creating expressions and different gRPC methods
//! require expressions in different forms. These traits are used to either translate a value into
//! a [spark::Expression] or into a [spark::expression::Literal].

use chrono::NaiveDateTime;

use crate::spark;

use crate::column::Column;
use crate::types::DataType;

pub struct VecExpression {
    pub(super) expr: Vec<spark::Expression>,
}

impl<T> FromIterator<T> for VecExpression
where
    T: Into<Column>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let expr = iter
            .into_iter()
            .map(Into::into)
            .map(|col| col.expression)
            .collect();

        VecExpression { expr }
    }
}

impl From<VecExpression> for Vec<spark::Expression> {
    fn from(value: VecExpression) -> Self {
        value.expr
    }
}

impl<'a> From<&'a str> for VecExpression {
    fn from(value: &'a str) -> Self {
        VecExpression {
            expr: vec![Column::from_str(value).expression],
        }
    }
}

impl From<String> for VecExpression {
    fn from(value: String) -> Self {
        VecExpression {
            expr: vec![Column::from_string(value).expression],
        }
    }
}

impl From<String> for spark::Expression {
    fn from(value: String) -> Self {
        Column::from(value).expression
    }
}

impl<'a> From<&'a str> for spark::Expression {
    fn from(value: &'a str) -> Self {
        Column::from(value).expression
    }
}

impl From<Column> for spark::Expression {
    fn from(value: Column) -> Self {
        value.expression
    }
}

/// Create a filter expression
pub trait ToFilterExpr {
    fn to_filter_expr(&self) -> Option<spark::Expression>;
}

impl ToFilterExpr for Column {
    fn to_filter_expr(&self) -> Option<spark::Expression> {
        Some(self.expression.clone())
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
        impl From<$type> for spark::expression::Literal {
            fn from(value: $type) -> spark::expression::Literal {
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::$inner_type(value)),
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
impl_to_literal!(String, String);

impl From<&[u8]> for spark::expression::Literal {
    fn from(value: &[u8]) -> Self {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Binary(Vec::from(
                value,
            ))),
        }
    }
}

impl From<i16> for spark::expression::Literal {
    fn from(value: i16) -> Self {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Short(value as i32)),
        }
    }
}

impl<'a> From<&'a str> for spark::expression::Literal {
    fn from(value: &'a str) -> Self {
        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::String(
                value.to_string(),
            )),
        }
    }
}

impl<Tz: chrono::TimeZone> From<chrono::DateTime<Tz>> for spark::expression::Literal {
    fn from(value: chrono::DateTime<Tz>) -> Self {
        // timestamps for spark have to be the microsends since 1/1/1970
        let timestamp = value.timestamp_micros();

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Timestamp(
                timestamp,
            )),
        }
    }
}

impl From<NaiveDateTime> for spark::expression::Literal {
    fn from(value: NaiveDateTime) -> Self {
        // timestamps for spark have to be the microsends since 1/1/1970
        let timestamp = value.and_utc().timestamp_micros();

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::TimestampNtz(
                timestamp,
            )),
        }
    }
}

impl From<chrono::NaiveDate> for spark::expression::Literal {
    fn from(value: chrono::NaiveDate) -> Self {
        // Spark works based on unix time. I.e. seconds since 1/1/1970
        // to get dates to work you have to do this math
        let days_since_unix_epoch =
            value.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Date(
                days_since_unix_epoch.num_days() as i32,
            )),
        }
    }
}

impl<T> From<Vec<T>> for spark::expression::Literal
where
    T: Into<spark::expression::Literal> + Clone,
    spark::DataType: From<T>,
{
    fn from(value: Vec<T>) -> Self {
        let element_type = Some(spark::DataType::from(
            value.first().expect("Array can not be empty").clone(),
        ));

        let elements = value.iter().map(|val| val.clone().into()).collect();

        let array_type = spark::expression::literal::Array {
            element_type,
            elements,
        };

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Array(array_type)),
        }
    }
}

impl<const N: usize, T> From<[T; N]> for spark::expression::Literal
where
    T: Into<spark::expression::Literal> + Clone,
    spark::DataType: From<T>,
{
    fn from(value: [T; N]) -> Self {
        let element_type = Some(spark::DataType::from(
            value.first().expect("Array can not be empty").clone(),
        ));

        let elements = value.iter().map(|val| val.clone().into()).collect();

        let array_type = spark::expression::literal::Array {
            element_type,
            elements,
        };

        spark::expression::Literal {
            literal_type: Some(spark::expression::literal::LiteralType::Array(array_type)),
        }
    }
}

impl From<&str> for spark::expression::cast::CastToType {
    fn from(value: &str) -> Self {
        spark::expression::cast::CastToType::TypeStr(value.to_string())
    }
}

impl From<String> for spark::expression::cast::CastToType {
    fn from(value: String) -> Self {
        spark::expression::cast::CastToType::TypeStr(value)
    }
}

impl From<DataType> for spark::expression::cast::CastToType {
    fn from(value: DataType) -> spark::expression::cast::CastToType {
        spark::expression::cast::CastToType::Type(value.into())
    }
}
