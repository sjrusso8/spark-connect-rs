//! Logical Plan representation

use std::collections::HashMap;
use std::sync::Mutex;

use crate::column::Column;
use crate::expressions::{ToExpr, ToFilterExpr, ToVecExpr};
use crate::spark;

use spark::relation::RelType;
use spark::Relation;
use spark::RelationCommon;

use spark::expression::ExprType;
use spark::set_operation::SetOpType;

/// Implements a struct to hold the current [Relation]
/// which represents an unresolved Logical Plan
#[derive(Clone, Debug)]
pub struct LogicalPlanBuilder {
    /// A [Relation] object that contains the unresolved
    /// logical plan
    pub relation: Relation,
    pub plan_id: i64,
}

#[allow(clippy::declare_interior_mutable_const)]
impl LogicalPlanBuilder {
    const NEXT_PLAN_ID: Mutex<i64> = Mutex::new(1);

    #[allow(clippy::clone_on_copy)]
    fn next_plan_id() -> i64 {
        let binding = LogicalPlanBuilder::NEXT_PLAN_ID;

        let mut next_plan_id = binding.lock().expect("Could not lock plan");

        let plan_id = next_plan_id.clone();

        *next_plan_id += 1;

        plan_id
    }

    /// Create a new Logical Plan from an initial [spark::Relation]
    pub fn new(relation: Relation) -> LogicalPlanBuilder {
        LogicalPlanBuilder {
            relation,
            plan_id: LogicalPlanBuilder::next_plan_id(),
        }
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
    pub fn build_plan_cmd(command_type: spark::command::CommandType) -> spark::Plan {
        spark::Plan {
            op_type: Some(spark::plan::OpType::Command(spark::Command {
                command_type: Some(command_type),
            })),
        }
    }

    /// Create a relation from an existing [LogicalPlanBuilder]
    /// this will add additional actions to the [Relation]
    pub fn from(rel_type: RelType) -> LogicalPlanBuilder {
        let plan_id = LogicalPlanBuilder::next_plan_id();

        let relation = Relation {
            common: Some(RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(plan_id),
            }),
            rel_type: Some(rel_type),
        };

        LogicalPlanBuilder { relation, plan_id }
    }

    pub fn alias(&mut self, alias: &str) -> LogicalPlanBuilder {
        let subquery = spark::SubqueryAlias {
            input: self.clone().relation_input(),
            alias: alias.to_string(),
            qualifier: vec![],
        };

        let alias_rel = RelType::SubqueryAlias(Box::new(subquery));

        LogicalPlanBuilder::from(alias_rel)
    }

    pub fn corr(&mut self, col1: &str, col2: &str) -> LogicalPlanBuilder {
        let corr = spark::StatCorr {
            input: self.clone().relation_input(),
            col1: col1.to_string(),
            col2: col2.to_string(),
            method: Some("pearson".to_string()),
        };

        let corr_rel = RelType::Corr(Box::new(corr));

        LogicalPlanBuilder::from(corr_rel)
    }

    pub fn cov(&mut self, col1: &str, col2: &str) -> LogicalPlanBuilder {
        let cov = spark::StatCov {
            input: self.clone().relation_input(),
            col1: col1.to_string(),
            col2: col2.to_string(),
        };

        let cov_rel = RelType::Cov(Box::new(cov));

        LogicalPlanBuilder::from(cov_rel)
    }

    pub fn crosstab(&mut self, col1: &str, col2: &str) -> LogicalPlanBuilder {
        let ctab = spark::StatCrosstab {
            input: self.clone().relation_input(),
            col1: col1.to_string(),
            col2: col2.to_string(),
        };

        let ctab_rel = RelType::Crosstab(Box::new(ctab));

        LogicalPlanBuilder::from(ctab_rel)
    }

    pub fn describe<'a, I>(&mut self, cols: Option<I>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = &'a str> + std::default::Default,
    {
        let desc = spark::StatDescribe {
            input: self.clone().relation_input(),
            cols: cols
                .unwrap_or_default()
                .into_iter()
                .map(|col| col.to_string())
                .collect(),
        };

        let desc_rel = RelType::Describe(Box::new(desc));

        LogicalPlanBuilder::from(desc_rel)
    }

    pub fn distinct(&mut self) -> LogicalPlanBuilder {
        self.drop_duplicates::<Vec<_>>(None)
    }

    pub fn drop<T: ToVecExpr>(&mut self, cols: T) -> LogicalPlanBuilder {
        let drop_expr = RelType::Drop(Box::new(spark::Drop {
            input: self.clone().relation_input(),
            columns: cols.to_vec_expr(),
            column_names: vec![],
        }));

        LogicalPlanBuilder::from(drop_expr)
    }

    pub fn drop_duplicates<'a, I>(&mut self, cols: Option<I>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = &'a str> + std::default::Default,
    {
        let drop_expr = match cols {
            Some(cols) => RelType::Deduplicate(Box::new(spark::Deduplicate {
                input: self.clone().relation_input(),
                column_names: cols.into_iter().map(|col| col.to_string()).collect(),
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

        LogicalPlanBuilder::from(drop_expr)
    }

    pub fn dropna<'a, I>(
        &mut self,
        how: &str,
        threshold: Option<i32>,
        subset: Option<I>,
    ) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = &'a str> + std::default::Default,
    {
        let mut min_non_nulls = match how {
            "all" => Some(1),
            "any" => None,
            &_ => panic!("'how' arg needs to be 'all' or 'any'"),
        };

        if let Some(threshold) = threshold {
            min_non_nulls = Some(threshold)
        };

        let dropna = spark::NaDrop {
            input: self.clone().relation_input(),
            cols: subset
                .unwrap_or_default()
                .into_iter()
                .map(|col| col.to_string())
                .collect(),
            min_non_nulls,
        };

        let dropna_rel = RelType::DropNa(Box::new(dropna));

        LogicalPlanBuilder::from(dropna_rel)
    }

    pub fn to_df<'a, I>(&mut self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = &'a str>,
    {
        let to_df = spark::ToDf {
            input: self.clone().relation_input(),
            column_names: cols.into_iter().map(|col| col.to_string()).collect(),
        };

        let to_df_rel = RelType::ToDf(Box::new(to_df));

        LogicalPlanBuilder::from(to_df_rel)
    }

    fn set_operation(
        &mut self,
        other: LogicalPlanBuilder,
        set_op_type: SetOpType,
        is_all: Option<bool>,
        by_name: Option<bool>,
        allow_missing_columns: Option<bool>,
    ) -> LogicalPlanBuilder {
        let set_op = spark::SetOperation {
            left_input: self.clone().relation_input(),
            right_input: other.clone().relation_input(),
            set_op_type: set_op_type.into(),
            is_all,
            by_name,
            allow_missing_columns,
        };

        let set_rel = RelType::SetOp(Box::new(set_op));

        LogicalPlanBuilder::from(set_rel)
    }

    #[allow(non_snake_case)]
    pub fn exceptAll(&mut self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Except,
            Some(true),
            Some(false),
            Some(false),
        )
    }

    #[allow(non_snake_case)]
    pub fn unionAll(&mut self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Union,
            Some(true),
            Some(false),
            Some(false),
        )
    }

    pub fn union(&mut self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.unionAll(other)
    }

    #[allow(non_snake_case)]
    pub fn unionByName(
        &mut self,
        other: LogicalPlanBuilder,
        allow_missing_columns: Option<bool>,
    ) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Union,
            Some(true),
            Some(true),
            allow_missing_columns,
        )
    }

    pub fn substract(&mut self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Except,
            Some(false),
            Some(false),
            Some(false),
        )
    }

    pub fn intersect(&mut self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Intersect,
            Some(false),
            Some(false),
            Some(false),
        )
    }

    #[allow(non_snake_case)]
    pub fn intersectAll(&mut self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Intersect,
            Some(true),
            Some(false),
            Some(false),
        )
    }

    pub fn filter<T: ToFilterExpr>(&mut self, condition: T) -> LogicalPlanBuilder {
        let rel_type = RelType::Filter(Box::new(spark::Filter {
            input: self.clone().relation_input(),
            condition: condition.to_filter_expr(),
        }));

        LogicalPlanBuilder::from(rel_type)
    }

    #[allow(non_snake_case)]
    pub fn freqItems<'a, I>(&mut self, cols: I, support: Option<f64>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = &'a str>,
    {
        let freq_items = spark::StatFreqItems {
            input: self.clone().relation_input(),
            cols: cols.into_iter().map(|col| col.to_string()).collect(),
            support,
        };

        let freq_items_rel = RelType::FreqItems(Box::new(freq_items));

        LogicalPlanBuilder::from(freq_items_rel)
    }

    pub fn hint<T: ToVecExpr>(&mut self, name: &str, parameters: Option<T>) -> LogicalPlanBuilder {
        let params = match parameters {
            Some(parameters) => parameters.to_vec_expr(),
            None => vec![],
        };

        let hint = spark::Hint {
            input: self.clone().relation_input(),
            name: name.to_string(),
            parameters: params,
        };

        let hint_rel = RelType::Hint(Box::new(hint));

        LogicalPlanBuilder::from(hint_rel)
    }

    pub fn join<'a, T, I>(
        &mut self,
        right: LogicalPlanBuilder,
        join_condition: Option<T>,
        join_type: spark::join::JoinType,
        using_columns: I,
    ) -> LogicalPlanBuilder
    where
        T: ToExpr,
        I: IntoIterator<Item = &'a str>,
    {
        let join_condition = join_condition.map(|join| join.to_expr());

        let join = spark::Join {
            left: self.clone().relation_input(),
            right: right.clone().relation_input(),
            join_condition,
            join_type: join_type.into(),
            using_columns: using_columns
                .into_iter()
                .map(|col| col.to_string())
                .collect(),
            join_data_type: None,
        };

        let join_rel = RelType::Join(Box::new(join));

        LogicalPlanBuilder::from(join_rel)
    }

    pub fn limit(&mut self, limit: i32) -> LogicalPlanBuilder {
        let limit_expr = RelType::Limit(Box::new(spark::Limit {
            input: self.clone().relation_input(),
            limit,
        }));

        LogicalPlanBuilder::from(limit_expr)
    }

    pub fn offset(&mut self, num: i32) -> LogicalPlanBuilder {
        let offset_expr = RelType::Offset(Box::new(spark::Offset {
            input: self.clone().relation_input(),
            offset: num,
        }));

        LogicalPlanBuilder::from(offset_expr)
    }

    pub fn repartition(
        &mut self,
        num_partitions: u32,
        shuffle: Option<bool>,
    ) -> LogicalPlanBuilder {
        let repart_expr = RelType::Repartition(Box::new(spark::Repartition {
            input: self.clone().relation_input(),
            num_partitions: num_partitions as i32,
            shuffle,
        }));

        LogicalPlanBuilder::from(repart_expr)
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

        LogicalPlanBuilder::from(sample_expr)
    }

    pub fn select<T: ToVecExpr>(&mut self, cols: T) -> LogicalPlanBuilder {
        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions: cols.to_vec_expr(),
            input: self.clone().relation_input(),
        }));

        LogicalPlanBuilder::from(rel_type)
    }

    pub fn select_expr<'a, I>(&mut self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = &'a str>,
    {
        let expressions = cols
            .into_iter()
            .map(|col| spark::Expression {
                expr_type: Some(spark::expression::ExprType::ExpressionString(
                    spark::expression::ExpressionString {
                        expression: col.to_string(),
                    },
                )),
            })
            .collect();

        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions,
            input: self.clone().relation_input(),
        }));

        LogicalPlanBuilder::from(rel_type)
    }

    pub fn sort<I>(&mut self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = Column>,
    {
        let order = cols
            .into_iter()
            .map(|col| {
                if let ExprType::SortOrder(ord) = col
                    .expression
                    .clone()
                    .expr_type
                    .expect("provided column set is not sortable")
                {
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

        LogicalPlanBuilder::from(sort_type)
    }

    #[allow(non_snake_case)]
    pub fn withColumn(&mut self, colName: &str, col: Column) -> LogicalPlanBuilder {
        let aliases: Vec<spark::expression::Alias> = vec![spark::expression::Alias {
            expr: Some(Box::new(col.to_expr())),
            name: vec![colName.to_string()],
            metadata: None,
        }];

        let with_col = RelType::WithColumns(Box::new(spark::WithColumns {
            input: self.clone().relation_input(),
            aliases,
        }));

        LogicalPlanBuilder::from(with_col)
    }

    #[allow(non_snake_case)]
    pub fn withColumns<I, K>(&mut self, colMap: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = (K, Column)>,
        K: ToString,
    {
        let aliases: Vec<spark::expression::Alias> = colMap
            .into_iter()
            .map(|(name, col)| spark::expression::Alias {
                expr: Some(Box::new(col.to_expr())),
                name: vec![name.to_string()],
                metadata: None,
            })
            .collect();

        let with_col = RelType::WithColumns(Box::new(spark::WithColumns {
            input: self.clone().relation_input(),
            aliases,
        }));

        LogicalPlanBuilder::from(with_col)
    }

    #[allow(non_snake_case)]
    pub fn withColumnsRenamed<I, K, V>(&mut self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let rename_columns_map: HashMap<String, String> = cols
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();

        let rename_expr = RelType::WithColumnsRenamed(Box::new(spark::WithColumnsRenamed {
            input: self.clone().relation_input(),
            rename_columns_map,
        }));

        LogicalPlanBuilder::from(rename_expr)
    }
}
