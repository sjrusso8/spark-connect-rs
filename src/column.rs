use std::convert::From;
use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Rem, Sub};

use crate::spark;

use crate::expressions::ToLiteralExpr;
use crate::functions::lit;
use crate::utils::invoke_func;

#[derive(Clone, Debug)]
pub struct Column {
    pub expression: spark::Expression,
}

impl From<spark::Expression> for Column {
    fn from(expression: spark::Expression) -> Self {
        Self { expression }
    }
}

impl From<&str> for Column {
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

impl Column {
    pub fn alias(&mut self, value: &str) -> Column {
        let alias = spark::expression::Alias {
            expr: Some(Box::new(self.expression.clone())),
            name: vec![value.to_string()],
            metadata: None,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::Alias(Box::new(alias))),
        };

        Column::from(expression)
    }

    pub fn name(&mut self, value: &str) -> Column {
        self.alias(value)
    }

    pub fn asc(&mut self) -> Column {
        self.asc_nulls_first()
    }

    pub fn asc_nulls_first(&mut self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression.clone())),
            direction: 1,
            null_ordering: 1,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn asc_nulls_last(&mut self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression.clone())),
            direction: 1,
            null_ordering: 2,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn cast(&mut self, to_type: &str) -> Column {
        let type_str = spark::expression::cast::CastToType::TypeStr(to_type.to_string());

        let cast = spark::expression::Cast {
            expr: Some(Box::new(self.expression.clone())),
            cast_to_type: Some(type_str),
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::Cast(Box::new(cast))),
        };

        Column::from(expression)
    }

    pub fn desc(&mut self) -> Column {
        self.desc_nulls_first()
    }

    pub fn desc_nulls_first(&mut self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression.clone())),
            direction: 2,
            null_ordering: 1,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn desc_nulls_last(&mut self) -> Column {
        let asc = spark::expression::SortOrder {
            child: Some(Box::new(self.expression.clone())),
            direction: 2,
            null_ordering: 2,
        };

        let expression = spark::Expression {
            expr_type: Some(spark::expression::ExprType::SortOrder(Box::new(asc))),
        };

        Column::from(expression)
    }

    pub fn isin<T: ToLiteralExpr>(&self, cols: Vec<T>) -> Column {
        let mut values = cols
            .iter()
            .map(|col| Column::from(col.to_literal_expr()))
            .collect::<Vec<Column>>();

        values.insert(0, self.clone());

        invoke_func("in", values)
    }

    pub fn contains<T: ToLiteralExpr>(&self, other: T) -> Column {
        let value = lit(other);

        invoke_func("contains", vec![self.clone(), value])
    }

    pub fn startswith<T: ToLiteralExpr>(&self, other: T) -> Column {
        let value = lit(other);

        invoke_func("startswith", vec![self.clone(), value])
    }

    pub fn endswith<T: ToLiteralExpr>(&self, other: T) -> Column {
        let value = lit(other);

        invoke_func("endswith", vec![self.clone(), value])
    }

    pub fn like<T: ToLiteralExpr>(&self, other: T) -> Column {
        let value = lit(other);

        invoke_func("like", vec![self.clone(), value])
    }

    pub fn ilike<T: ToLiteralExpr>(&self, other: T) -> Column {
        let value = lit(other);

        invoke_func("ilike", vec![self.clone(), value])
    }

    pub fn rlike<T: ToLiteralExpr>(&self, other: T) -> Column {
        let value = lit(other);

        invoke_func("rlike", vec![self.clone(), value])
    }

    #[allow(non_snake_case)]
    pub fn isNull(&self) -> Column {
        invoke_func("isnull", self.clone())
    }

    #[allow(non_snake_case)]
    pub fn isNotNull(&self) -> Column {
        invoke_func("isnotnull", self.clone())
    }

    #[allow(non_snake_case)]
    pub fn isNaN(&self) -> Column {
        invoke_func("isNaN", self.clone())
    }
}
