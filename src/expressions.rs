use crate::spark;

use crate::column::Column;

pub trait ToExpressionVec {
    fn to_expression_vec(&self) -> Vec<spark::Expression>;
}

impl ToExpressionVec for Vec<Column> {
    fn to_expression_vec(&self) -> Vec<spark::Expression> {
        self.iter().map(|col| col.expression.clone()).collect()
    }
}
