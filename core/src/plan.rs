//! Logical Plan representation

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;

use crate::errors::SparkError;
use crate::expressions::{ToFilterExpr, VecExpression};
use crate::spark;

use arrow::array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use spark::relation::RelType;
use spark::Relation;
use spark::RelationCommon;

use spark::aggregate::GroupType;
use spark::set_operation::SetOpType;

use crate::column::Column;

/// Implements a struct to hold the current [Relation]
/// which represents an unresolved Logical Plan
#[derive(Clone, Debug)]
pub struct LogicalPlanBuilder {
    pub(crate) relation: spark::Relation,
    pub(crate) plan_id: i64,
}

static NEXT_PLAN_ID: AtomicI64 = AtomicI64::new(1);

impl LogicalPlanBuilder {
    fn next_plan_id() -> i64 {
        NEXT_PLAN_ID.fetch_add(1, SeqCst)
    }

    /// Create a new Logical Plan from an initial [spark::Relation]
    pub fn new(relation: Relation) -> LogicalPlanBuilder {
        LogicalPlanBuilder {
            relation,
            plan_id: LogicalPlanBuilder::next_plan_id(),
        }
    }

    pub fn relation(self) -> spark::Relation {
        self.relation
    }

    pub fn plan_id(self) -> i64 {
        self.plan_id
    }

    pub fn relation_input(self) -> Option<Box<Relation>> {
        Some(Box::new(self.relation))
    }

    /// Build the Spark [spark::Plan] for a [Relation]
    pub fn plan_root(self) -> spark::Plan {
        spark::Plan {
            op_type: Some(spark::plan::OpType::Root(self.relation())),
        }
    }

    /// Build the Spark [spark::Plan] for a [spark::command::CommandType]
    pub fn plan_cmd(command_type: spark::command::CommandType) -> spark::Plan {
        spark::Plan {
            op_type: Some(spark::plan::OpType::Command(spark::Command {
                command_type: Some(command_type),
            })),
        }
    }

    /// Create a relation from an existing [LogicalPlanBuilder]
    /// this will add additional actions to the [Relation]

    pub fn alias(self, alias: &str) -> LogicalPlanBuilder {
        let subquery = spark::SubqueryAlias {
            input: self.relation_input(),
            alias: alias.to_string(),
            qualifier: vec![],
        };

        let alias_rel = RelType::SubqueryAlias(Box::new(subquery));

        LogicalPlanBuilder::from(alias_rel)
    }

    pub fn aggregate<I, S>(
        input: LogicalPlanBuilder,
        group_type: GroupType,
        grouping_cols: Vec<spark::Expression>,
        agg_expression: I,
        pivot_col: Option<spark::Expression>,
        pivot_vals: Option<Vec<spark::expression::Literal>>,
    ) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        let pivot = match group_type {
            GroupType::Pivot => Some(spark::aggregate::Pivot {
                col: pivot_col,
                values: pivot_vals.unwrap_or_default(),
            }),
            _ => None,
        };

        let agg = spark::Aggregate {
            input: input.relation_input(),
            group_type: group_type.into(),
            grouping_expressions: grouping_cols,
            aggregate_expressions: VecExpression::from_iter(agg_expression).expr,
            pivot,
        };

        let agg_rel = RelType::Aggregate(Box::new(agg));

        LogicalPlanBuilder::from(agg_rel)
    }

    pub fn unpivot(
        self,
        ids: Vec<spark::Expression>,
        values: Option<Vec<spark::Expression>>,
        variable_column_name: &str,
        value_column_name: &str,
    ) -> LogicalPlanBuilder {
        let unpivot_values = values.map(|val| spark::unpivot::Values { values: val });

        let unpivot = spark::Unpivot {
            input: self.relation_input(),
            ids,
            values: unpivot_values,
            variable_column_name: variable_column_name.to_string(),
            value_column_name: value_column_name.to_string(),
        };

        LogicalPlanBuilder::from(RelType::Unpivot(Box::new(unpivot)))
    }

    pub fn local_relation(batch: &RecordBatch) -> Result<LogicalPlanBuilder, SparkError> {
        let serialized = serialize(batch)?;

        let local_rel = spark::LocalRelation {
            data: Some(serialized),
            schema: None,
        };

        let local_rel = RelType::LocalRelation(local_rel);

        Ok(LogicalPlanBuilder::from(local_rel))
    }

    pub fn corr(self, col1: impl AsRef<str>, col2: impl AsRef<str>) -> LogicalPlanBuilder {
        let corr = spark::StatCorr {
            input: self.relation_input(),
            col1: col1.as_ref().to_string(),
            col2: col2.as_ref().to_string(),
            method: Some("pearson".to_string()),
        };

        let corr_rel = RelType::Corr(Box::new(corr));

        LogicalPlanBuilder::from(corr_rel)
    }

    pub fn cov(self, col1: impl AsRef<str>, col2: impl AsRef<str>) -> LogicalPlanBuilder {
        let cov = spark::StatCov {
            input: self.relation_input(),
            col1: col1.as_ref().to_string(),
            col2: col2.as_ref().to_string(),
        };

        let cov_rel = RelType::Cov(Box::new(cov));

        LogicalPlanBuilder::from(cov_rel)
    }

    pub fn crosstab(self, col1: &str, col2: &str) -> LogicalPlanBuilder {
        let ctab = spark::StatCrosstab {
            input: self.relation_input(),
            col1: col1.to_string(),
            col2: col2.to_string(),
        };

        let ctab_rel = RelType::Crosstab(Box::new(ctab));

        LogicalPlanBuilder::from(ctab_rel)
    }

    pub fn describe<I, T>(self, cols: Option<I>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let cols = match cols {
            Some(cols) => cols.into_iter().map(|c| c.as_ref().to_string()).collect(),
            None => vec![],
        };

        let desc = spark::StatDescribe {
            input: self.relation_input(),
            cols,
        };

        let desc_rel = RelType::Describe(Box::new(desc));

        LogicalPlanBuilder::from(desc_rel)
    }

    pub fn distinct(self) -> LogicalPlanBuilder {
        self.drop_duplicates::<Vec<_>, String>(None, false)
    }

    pub fn drop<I, S>(self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        let drop_expr = RelType::Drop(Box::new(spark::Drop {
            input: self.relation_input(),
            columns: VecExpression::from_iter(cols).expr,
            column_names: vec![],
        }));

        LogicalPlanBuilder::from(drop_expr)
    }

    pub fn drop_duplicates<I, T>(
        self,
        cols: Option<I>,
        within_watermark: bool,
    ) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let drop_expr = match cols {
            Some(cols) => spark::Deduplicate {
                input: self.relation_input(),
                column_names: cols
                    .into_iter()
                    .map(|col| col.as_ref().to_string())
                    .collect(),
                all_columns_as_keys: Some(false),
                within_watermark: Some(within_watermark),
            },

            None => spark::Deduplicate {
                input: self.relation_input(),
                column_names: vec![],
                all_columns_as_keys: Some(true),
                within_watermark: Some(within_watermark),
            },
        };

        let rel_type = RelType::Deduplicate(Box::new(drop_expr));

        LogicalPlanBuilder::from(rel_type)
    }

    // TODO! this should probably be an enum
    pub fn dropna<I, T>(
        self,
        how: &str,
        threshold: Option<i32>,
        subset: Option<I>,
    ) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let mut min_non_nulls = match how {
            "all" => Some(1),
            "any" => None,
            &_ => panic!("'how' arg needs to be 'all' or 'any'"),
        };

        if let Some(threshold) = threshold {
            min_non_nulls = Some(threshold)
        };

        let cols = match subset {
            Some(cols) => cols.into_iter().map(|c| c.as_ref().to_string()).collect(),
            None => vec![],
        };

        let dropna = spark::NaDrop {
            input: self.relation_input(),
            cols,
            min_non_nulls,
        };

        let dropna_rel = RelType::DropNa(Box::new(dropna));

        LogicalPlanBuilder::from(dropna_rel)
    }

    pub fn fillna<I, T, L>(self, cols: Option<I>, values: T) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item: AsRef<str>>,
        T: IntoIterator<Item = L>,
        L: Into<spark::expression::Literal>,
    {
        let cols: Vec<String> = match cols {
            Some(cols) => cols.into_iter().map(|v| v.as_ref().to_string()).collect(),
            None => vec![],
        };

        let values: Vec<spark::expression::Literal> =
            values.into_iter().map(|v| v.into()).collect();

        let fillna = RelType::FillNa(Box::new(spark::NaFill {
            input: self.relation_input(),
            cols,
            values,
        }));

        LogicalPlanBuilder::from(fillna)
    }

    pub fn to_df<I>(self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        let to_df = spark::ToDf {
            input: self.relation_input(),
            column_names: cols
                .into_iter()
                .map(|col| col.as_ref().to_string())
                .collect(),
        };

        let to_df_rel = RelType::ToDf(Box::new(to_df));

        LogicalPlanBuilder::from(to_df_rel)
    }

    fn set_operation(
        self,
        other: LogicalPlanBuilder,
        set_op_type: SetOpType,
        is_all: Option<bool>,
        by_name: Option<bool>,
        allow_missing_columns: Option<bool>,
    ) -> LogicalPlanBuilder {
        let set_op = spark::SetOperation {
            left_input: self.relation_input(),
            right_input: other.relation_input(),
            set_op_type: set_op_type.into(),
            is_all,
            by_name,
            allow_missing_columns,
        };

        let set_rel = RelType::SetOp(Box::new(set_op));

        LogicalPlanBuilder::from(set_rel)
    }

    pub fn except_all(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Except,
            Some(true),
            Some(false),
            Some(false),
        )
    }

    pub fn union_all(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Union,
            Some(true),
            Some(false),
            Some(false),
        )
    }

    pub fn union(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.union_all(other)
    }

    pub fn union_by_name(
        self,
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

    pub fn substract(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Except,
            Some(false),
            Some(false),
            Some(false),
        )
    }

    pub fn intersect(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Intersect,
            Some(false),
            Some(false),
            Some(false),
        )
    }

    pub fn intersect_all(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder {
        self.set_operation(
            other,
            SetOpType::Intersect,
            Some(true),
            Some(false),
            Some(false),
        )
    }

    pub fn filter<T: ToFilterExpr>(self, condition: T) -> LogicalPlanBuilder {
        let rel_type = RelType::Filter(Box::new(spark::Filter {
            input: self.relation_input(),
            condition: condition.to_filter_expr(),
        }));

        LogicalPlanBuilder::from(rel_type)
    }

    pub fn approx_quantile<I, P>(
        self,
        cols: I,
        probabilities: P,
        relative_error: f64,
    ) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item: AsRef<str>>,
        P: IntoIterator<Item = f64>,
    {
        let approx_quantile = spark::StatApproxQuantile {
            input: self.relation_input(),
            cols: cols
                .into_iter()
                .map(|col| col.as_ref().to_string())
                .collect(),
            probabilities: probabilities.into_iter().collect(),
            relative_error,
        };

        let approx_quantile_rel = RelType::ApproxQuantile(Box::new(approx_quantile));

        LogicalPlanBuilder::from(approx_quantile_rel)
    }

    pub fn freq_items<I, S>(self, cols: I, support: Option<f64>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let freq_items = spark::StatFreqItems {
            input: self.relation_input(),
            cols: cols
                .into_iter()
                .map(|col| col.as_ref().to_string())
                .collect(),
            support,
        };

        let freq_items_rel = RelType::FreqItems(Box::new(freq_items));

        LogicalPlanBuilder::from(freq_items_rel)
    }

    pub fn hint<I, S>(self, name: &str, parameters: Option<I>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        let parameters = match parameters {
            Some(parameters) => VecExpression::from_iter(parameters).expr,
            None => vec![],
        };

        let hint = spark::Hint {
            input: self.relation_input(),
            name: name.to_string(),
            parameters,
        };

        let hint_rel = RelType::Hint(Box::new(hint));

        LogicalPlanBuilder::from(hint_rel)
    }

    pub fn join<'a, T, I>(
        self,
        right: LogicalPlanBuilder,
        join_condition: Option<T>,
        join_type: spark::join::JoinType,
        using_columns: I,
    ) -> LogicalPlanBuilder
    where
        T: Into<spark::Expression>,
        I: IntoIterator<Item = &'a str>,
    {
        let join_condition = join_condition.map(|join| join.into());

        let join = spark::Join {
            left: self.relation_input(),
            right: right.relation_input(),
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

    pub fn limit(self, limit: i32) -> LogicalPlanBuilder {
        let limit_expr = RelType::Limit(Box::new(spark::Limit {
            input: self.relation_input(),
            limit,
        }));

        LogicalPlanBuilder::from(limit_expr)
    }

    pub fn offset(self, num: i32) -> LogicalPlanBuilder {
        let offset_expr = RelType::Offset(Box::new(spark::Offset {
            input: self.relation_input(),
            offset: num,
        }));

        LogicalPlanBuilder::from(offset_expr)
    }

    pub fn project<I, S>(self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions: VecExpression::from_iter(cols).expr,
            input: self.relation_input(),
        }));

        LogicalPlanBuilder::from(rel_type)
    }

    pub fn repartition(self, num_partitions: u32, shuffle: Option<bool>) -> LogicalPlanBuilder {
        let repart_expr = RelType::Repartition(Box::new(spark::Repartition {
            input: self.relation_input(),
            num_partitions: num_partitions as i32,
            shuffle,
        }));

        LogicalPlanBuilder::from(repart_expr)
    }

    pub fn repartition_by_range<I, S>(
        self,
        num_partitions: Option<i32>,
        cols: I,
    ) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        let repart_expr =
            RelType::RepartitionByExpression(Box::new(spark::RepartitionByExpression {
                input: self.relation_input(),
                num_partitions,
                partition_exprs: VecExpression::from_iter(cols).expr,
            }));

        LogicalPlanBuilder::from(repart_expr)
    }

    pub fn replace<I, T, L>(self, to_replace: T, value: T, subset: Option<I>) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item: AsRef<str>>,
        T: IntoIterator<Item = L>,
        L: Into<spark::expression::Literal>,
    {
        let cols: Vec<String> = match subset {
            Some(subset) => subset.into_iter().map(|v| v.as_ref().to_string()).collect(),
            None => vec![],
        };

        let replacements = to_replace
            .into_iter()
            .zip(value)
            .map(|(a, b)| spark::na_replace::Replacement {
                old_value: Some(a.into()),
                new_value: Some(b.into()),
            })
            .collect();

        let replace = spark::NaReplace {
            input: self.relation_input(),
            cols,
            replacements,
        };

        let replace_expr = RelType::Replace(Box::new(replace));

        LogicalPlanBuilder::from(replace_expr)
    }

    pub fn sample(
        self,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: Option<bool>,
        seed: Option<i64>,
        deterministic_order: bool,
    ) -> LogicalPlanBuilder {
        let sample_expr = RelType::Sample(Box::new(spark::Sample {
            input: self.relation_input(),
            lower_bound,
            upper_bound,
            with_replacement,
            seed,
            deterministic_order,
        }));

        LogicalPlanBuilder::from(sample_expr)
    }

    pub fn sample_by<K, I, T>(self, col: T, fractions: I, seed: i64) -> LogicalPlanBuilder
    where
        K: Into<spark::expression::Literal>,
        T: Into<spark::Expression>,
        I: IntoIterator<Item = (K, f64)>,
    {
        let fractions = fractions
            .into_iter()
            .map(|(k, v)| spark::stat_sample_by::Fraction {
                stratum: Some(k.into()),
                fraction: v,
            })
            .collect();

        let sample_expr = RelType::SampleBy(Box::new(spark::StatSampleBy {
            input: self.relation_input(),
            col: Some(col.into()),
            fractions,
            seed: Some(seed),
        }));

        LogicalPlanBuilder::from(sample_expr)
    }

    pub fn select_expr<I>(self, cols: I) -> LogicalPlanBuilder
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        let expressions = cols
            .into_iter()
            .map(|col| spark::Expression {
                expr_type: Some(spark::expression::ExprType::ExpressionString(
                    spark::expression::ExpressionString {
                        expression: col.as_ref().to_string(),
                    },
                )),
            })
            .collect();

        let rel_type = RelType::Project(Box::new(spark::Project {
            expressions,
            input: self.relation_input(),
        }));

        LogicalPlanBuilder::from(rel_type)
    }

    pub fn sort<I, T>(self, cols: I, is_global: bool) -> LogicalPlanBuilder
    where
        T: Into<Column>,
        I: IntoIterator<Item = T>,
    {
        let order = sort_order(cols);
        let sort_type = RelType::Sort(Box::new(spark::Sort {
            order,
            input: self.relation_input(),
            is_global: Some(is_global),
        }));

        LogicalPlanBuilder::from(sort_type)
    }

    pub fn summary<T, I>(self, statistics: Option<I>) -> LogicalPlanBuilder
    where
        T: AsRef<str>,
        I: IntoIterator<Item = T>,
    {
        let statistics = match statistics {
            Some(stats) => stats.into_iter().map(|s| s.as_ref().to_string()).collect(),
            None => vec![
                "count".to_string(),
                "mean".to_string(),
                "stddev".to_string(),
                "min".to_string(),
                "25%".to_string(),
                "50%".to_string(),
                "75%".to_string(),
                "max".to_string(),
            ],
        };

        let stats = RelType::Summary(Box::new(spark::StatSummary {
            input: self.relation_input(),
            statistics,
        }));

        LogicalPlanBuilder::from(stats)
    }

    pub fn with_column(self, col_name: &str, col: Column) -> LogicalPlanBuilder {
        let aliases: Vec<spark::expression::Alias> = vec![spark::expression::Alias {
            expr: Some(Box::new(col.expression)),
            name: vec![col_name.to_string()],
            metadata: None,
        }];

        let with_col = RelType::WithColumns(Box::new(spark::WithColumns {
            input: self.relation_input(),
            aliases,
        }));

        LogicalPlanBuilder::from(with_col)
    }

    pub fn with_columns<K, I, N, M>(self, col_map: I, metadata: Option<M>) -> LogicalPlanBuilder
    where
        K: AsRef<str>,
        I: IntoIterator<Item = (K, Column)>,
        N: AsRef<str>,
        M: IntoIterator<Item = N>,
    {
        let mut aliases: Vec<spark::expression::Alias> = col_map
            .into_iter()
            .map(|(name, col)| spark::expression::Alias {
                expr: Some(Box::new(col.expression)),
                name: vec![name.as_ref().to_string()],
                metadata: None,
            })
            .collect();

        if let Some(meta_iter) = metadata {
            aliases
                .iter_mut()
                .zip(meta_iter)
                .for_each(|(alias, meta)| alias.metadata = Some(meta.as_ref().to_string()));
        }

        let with_col = RelType::WithColumns(Box::new(spark::WithColumns {
            input: self.relation_input(),
            aliases,
        }));

        LogicalPlanBuilder::from(with_col)
    }

    pub fn with_columns_renamed<I, K, V>(self, cols: I) -> LogicalPlanBuilder
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
            input: self.relation_input(),
            rename_columns_map,
        }));

        LogicalPlanBuilder::from(rename_expr)
    }

    pub fn with_watermark<T, D>(self, event_time: T, delay_threshold: D) -> LogicalPlanBuilder
    where
        T: AsRef<str>,
        D: AsRef<str>,
    {
        let watermark_expr = RelType::WithWatermark(Box::new(spark::WithWatermark {
            input: self.relation_input(),
            event_time: event_time.as_ref().to_string(),
            delay_threshold: delay_threshold.as_ref().to_string(),
        }));

        LogicalPlanBuilder::from(watermark_expr)
    }
}

impl From<spark::relation::RelType> for LogicalPlanBuilder {
    fn from(rel_type: RelType) -> LogicalPlanBuilder {
        let plan_id = LogicalPlanBuilder::next_plan_id();

        let relation = Relation {
            common: Some(RelationCommon {
                source_info: "".to_string(),
                plan_id: Some(plan_id),
            }),
            rel_type: Some(rel_type),
        };

        LogicalPlanBuilder { relation, plan_id }
    }
}

pub(crate) fn sort_order<I, S>(cols: I) -> Vec<spark::expression::SortOrder>
where
    I: IntoIterator<Item = S>,
    S: Into<Column>,
{
    VecExpression::from_iter(cols)
        .expr
        .into_iter()
        .map(|col| match col.expr_type.clone().unwrap() {
            spark::expression::ExprType::SortOrder(ord) => *ord,
            _ => spark::expression::SortOrder {
                child: Some(Box::new(col)),
                direction: 1,
                null_ordering: 1,
            },
        })
        .collect()
}

pub(crate) fn serialize(batch: &RecordBatch) -> Result<Vec<u8>, SparkError> {
    let buffer: Vec<u8> = Vec::new();
    let schema = &batch.schema();

    let mut writer = StreamWriter::try_new(buffer, schema)?;
    writer.write(batch)?;

    Ok(writer.into_inner()?)
}
