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

pub fn sort_order<I>(cols: I) -> Vec<spark::expression::SortOrder>
where
    I: IntoIterator<Item = Column>,
{
    cols.into_iter()
        .map(|col| match col.clone().expression.expr_type.unwrap() {
            spark::expression::ExprType::SortOrder(ord) => *ord,
            _ => spark::expression::SortOrder {
                child: Some(Box::new(col.expression)),
                direction: 1,
                null_ordering: 1,
            },
        })
        .collect()
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

#[macro_export]
macro_rules! impl_to_data_type {
    ($type:ty, $inner_type:ident) => {
        impl ToDataType for $type {
            fn to_proto_type(&self) -> spark::DataType {
                spark::DataType {
                    kind: Some(spark::data_type::Kind::$inner_type(
                        spark::data_type::$inner_type {
                            type_variation_reference: 0,
                        },
                    )),
                }
            }
        }
    };
}

#[macro_export]
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
