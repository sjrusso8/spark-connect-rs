use crate::spark;

use crate::column::Column;
use crate::expressions::ToVecExpr;

pub fn invoke_func<T: ToVecExpr>(name: &str, args: T) -> Column {
    Column::from(spark::Expression {
        expr_type: Some(spark::expression::ExprType::UnresolvedFunction(
            spark::expression::UnresolvedFunction {
                function_name: name.to_string(),
                arguments: args.to_vec_expr(),
                is_distinct: false,
                is_user_defined_function: false,
            },
        )),
    })
}

#[macro_export]
macro_rules! generate_functions {
    (no_args: $($func_name:ident),*) => {
        $(
            pub fn $func_name() -> Column {
                let empty_args: Vec<Column> = vec![];
                invoke_func(stringify!($func_name), empty_args)
            }
        )*
    };
    (one_col: $($func_name:ident),*) => {
        $(
            pub fn $func_name<T: expressions::ToExpr>(col: T) -> Column
            where
                Vec<T>: expressions::ToVecExpr,
            {
                invoke_func(stringify!($func_name), vec![col])
            }
        )*
    };
    (two_cols: $($func_name:ident),*) => {
        $(
            pub fn $func_name<T: expressions::ToExpr>(col1: T, col2: T) -> Column
            where
                Vec<T>: expressions::ToVecExpr,
            {
                invoke_func(stringify!($func_name), vec![col1, col2])
            }
        )*
    };
    (multiple_cols: $($func_name:ident),*) => {
        $(
            pub fn $func_name<T: expressions::ToVecExpr>(cols: T) -> Column
            {
                invoke_func(stringify!($func_name), cols)
            }
        )*
    };
}
