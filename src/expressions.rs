//! Traits for converting Rust Types to Spark Connect Expression Types

use crate::spark;

use crate::column::Column;
use crate::types::ToDataType;

use crate::impl_to_literal;

pub trait ToExpr {
    fn to_expr(&self) -> spark::Expression;
}

impl ToExpr for &str {
    fn to_expr(&self) -> spark::Expression {
        Column::from(*self).expression.clone()
    }
}

impl ToExpr for Column {
    fn to_expr(&self) -> spark::Expression {
        self.expression.clone()
    }
}

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

impl<T> ToVecExpr for Vec<T>
where
    T: ToExpr,
{
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        self.iter().map(|col| col.to_expr()).collect()
    }
}

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

pub trait ToLiteral {
    fn to_literal(&self) -> spark::expression::Literal;
}

impl_to_literal!(i32, Integer);
impl_to_literal!(i64, Long);
impl_to_literal!(f32, Float);
impl_to_literal!(f64, Double);

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
