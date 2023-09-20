use crate::spark;

use spark::relation::RelType;
use spark::Relation;
use spark::RelationCommon;

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
}
