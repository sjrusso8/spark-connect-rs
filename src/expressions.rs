use crate::spark;

use crate::column::Column;

pub trait ToVecExpr {
    fn to_vec_expr(&self) -> Vec<spark::Expression>;
}

pub trait ToExpr {
    fn to_expr(&self) -> spark::Expression;
}

pub trait ToFilterExpr {
    fn to_filter_expr(&self) -> Option<spark::Expression>;
}

pub trait ToLiteralExpr {
    fn to_literal_expr(&self) -> spark::Expression;
}

impl ToVecExpr for Vec<Column> {
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        self.iter().map(|col| col.expression.clone()).collect()
    }
}

impl ToVecExpr for Vec<&str> {
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        self.iter()
            .map(|col| Column::from(*col).expression.clone())
            .collect()
    }
}

impl ToVecExpr for Column {
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        vec![self.expression.clone()]
    }
}

impl ToVecExpr for &str {
    fn to_vec_expr(&self) -> Vec<spark::Expression> {
        vec![self.to_expr()]
    }
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

impl ToLiteralExpr for Column {
    fn to_literal_expr(&self) -> spark::Expression {
        self.expression.clone()
    }
}

impl ToLiteralExpr for i32 {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Integer(*self)),
                },
            )),
        }
    }
}

impl ToLiteralExpr for i64 {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Long(*self)),
                },
            )),
        }
    }
}

impl ToLiteralExpr for f32 {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Float(*self)),
                },
            )),
        }
    }
}

impl ToLiteralExpr for f64 {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Double(*self)),
                },
            )),
        }
    }
}

impl ToLiteralExpr for bool {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::Boolean(*self)),
                },
            )),
        }
    }
}

impl ToLiteralExpr for String {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::String(
                        self.clone(),
                    )),
                },
            )),
        }
    }
}

impl ToLiteralExpr for &str {
    fn to_literal_expr(&self) -> spark::Expression {
        spark::Expression {
            expr_type: Some(spark::expression::ExprType::Literal(
                spark::expression::Literal {
                    literal_type: Some(spark::expression::literal::LiteralType::String(
                        self.to_string(),
                    )),
                },
            )),
        }
    }
}
