use crate::spark;

use std::convert::From;

pub struct Column {
    pub expression: spark::Expression,
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

        Column { expression }
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

        Column { expression }
    }

    pub fn name(&mut self, value: &str) -> Column {
        self.alias(value)
    }
}
