use std::collections::HashMap;

use crate::column::Column;
use crate::expressions::{ToFilterExpr, ToVecExpr};
use crate::spark;

use spark::relation::RelType;
use spark::Relation;
use spark::RelationCommon;

use spark::expression::ExprType;

/// Implements a struct to hold the current [Relation]
/// which represents an unresolved Logical Plan
#[derive(Clone, Debug)]
pub struct LogicalPlanBuilder {
    /// A [Relation] object that contains the unresolved
    /// logical plan
    pub relation: Relation,
}

impl LogicalPlanBuilder {
    /// Create a new Logical Plan from an initial [Relation]
    pub fn new(relation: Relation) -> Self {
        Self { relation }
    }

    pub fn relation_input(self) -> Option<Box<Relation>> {
        Some(Box::new(self.relation))
    }

    /// Build the Spark [spark::Plan] for a [Relation]
    pub fn build_plan_root(self) -> spark::Plan {
        spark::Plan {
            op_type: Some(spark::plan::OpType::Root(self.relation)),
        }
    }

    /// Build the Spark [spark::Plan] for a [spark::command::CommandType]
    pub fn build_plan_cmd(self, command_type: spark::command::CommandType) -> spark::Plan {
        let cmd = spark::Command {
            command_type: Some(command_type),
        };

        spark::Plan {
            op_type: Some(spark::plan::OpType::Command(cmd)),
        }
    }

    /// Create a relation from an existing [LogicalPlanBuilder]
    /// this will add additional actions to the [Relation]
    pub fn from(&mut self, rel_type: RelType) -> LogicalPlanBuilder {
        let relation = Relation {
            common: Some(RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(1),
            }),
            rel_type: Some(rel_type),
        };

        LogicalPlanBuilder { relation }
    }

    pub fn select<T: ToVecExpr>(&mut self, cols: T) -> LogicalPlanBuilder {
        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions: cols.to_vec_expr(),
            input: self.clone().relation_input(),
        }));

        self.from(rel_type)
    }

    pub fn select_expr(&mut self, cols: Vec<&str>) -> LogicalPlanBuilder {
        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions: cols.to_vec_expr(),
            input: self.clone().relation_input(),
        }));

        self.from(rel_type)
    }

    pub fn filter<T: ToFilterExpr>(&mut self, condition: T) -> LogicalPlanBuilder {
        let rel_type = RelType::Filter(Box::new(spark::Filter {
            input: self.clone().relation_input(),
            condition: condition.to_filter_expr(),
        }));

        self.from(rel_type)
    }

    pub fn contains(&mut self, condition: Column) -> LogicalPlanBuilder {
        let rel_type = RelType::Filter(Box::new(spark::Filter {
            input: self.clone().relation_input(),
            condition: Some(condition.expression.clone()),
        }));

        self.from(rel_type)
    }

    pub fn limit(&mut self, limit: i32) -> LogicalPlanBuilder {
        let limit_expr = RelType::Limit(Box::new(spark::Limit {
            input: self.clone().relation_input(),
            limit,
        }));

        self.from(limit_expr)
    }

    pub fn drop_duplicates(&mut self, cols: Option<Vec<&str>>) -> LogicalPlanBuilder {
        let drop_expr = match cols {
            Some(cols) => RelType::Deduplicate(Box::new(spark::Deduplicate {
                input: self.clone().relation_input(),
                column_names: cols.iter().map(|col| col.to_string()).collect(),
                all_columns_as_keys: Some(false),
                within_watermark: Some(false),
            })),

            None => RelType::Deduplicate(Box::new(spark::Deduplicate {
                input: self.clone().relation_input(),
                column_names: vec![],
                all_columns_as_keys: Some(true),
                within_watermark: Some(false),
            })),
        };

        self.from(drop_expr)
    }

    pub fn with_columns_renamed(&mut self, cols: HashMap<String, String>) -> LogicalPlanBuilder {
        let rename_expr = RelType::WithColumnsRenamed(Box::new(spark::WithColumnsRenamed {
            input: self.clone().relation_input(),
            rename_columns_map: cols,
        }));

        self.from(rename_expr)
    }

    pub fn drop(&mut self, cols: Vec<String>) -> LogicalPlanBuilder {
        let drop_expr = RelType::Drop(Box::new(spark::Drop {
            input: self.clone().relation_input(),
            columns: vec![],
            column_names: cols,
        }));

        self.from(drop_expr)
    }

    pub fn sample(
        &mut self,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: Option<bool>,
        seed: Option<i64>,
    ) -> LogicalPlanBuilder {
        let sample_expr = RelType::Sample(Box::new(spark::Sample {
            input: self.clone().relation_input(),
            lower_bound,
            upper_bound,
            with_replacement,
            seed,
            deterministic_order: false,
        }));

        self.from(sample_expr)
    }

    pub fn repartition(
        &mut self,
        num_partitions: i32,
        shuffle: Option<bool>,
    ) -> LogicalPlanBuilder {
        let repart_expr = RelType::Repartition(Box::new(spark::Repartition {
            input: self.clone().relation_input(),
            num_partitions,
            shuffle,
        }));

        self.from(repart_expr)
    }

    pub fn offset(&mut self, num: i32) -> LogicalPlanBuilder {
        let offset_expr = RelType::Offset(Box::new(spark::Offset {
            input: self.clone().relation_input(),
            offset: num,
        }));

        self.from(offset_expr)
    }

    pub fn sort(&mut self, cols: Vec<Column>) -> LogicalPlanBuilder {
        let order = cols
            .iter()
            .map(|col| {
                if let ExprType::SortOrder(ord) = col.expression.clone().expr_type.unwrap() {
                    *ord
                } else {
                    // TODO don't make this a panic but actually raise an error
                    panic!("not sortable")
                }
            })
            .collect();

        let sort_type = RelType::Sort(Box::new(spark::Sort {
            order,
            input: self.clone().relation_input(),
            is_global: None,
        }));

        self.from(sort_type)
    }
}
