//! Spark Catalog representation through which the user may create, drop, alter or query underlying databases, tables, functions, etc.

use arrow::array::RecordBatch;

use crate::plan::LogicalPlanBuilder;
use crate::session::SparkSession;
use crate::spark;

#[derive(Debug, Clone)]
pub struct Catalog {
    spark_session: SparkSession,
}

impl Catalog {
    pub fn new(spark_session: SparkSession) -> Self {
        Self { spark_session }
    }

    /// Returns the current default catalog in this session
    #[allow(non_snake_case)]
    pub async fn currentCatalog(&mut self) -> String {
        let cat_type = Some(spark::catalog::CatType::CurrentCatalog(
            spark::CurrentCatalog {},
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::from(rel_type).clone().build_plan_root();

        self.spark_session
            .clone()
            .consume_plan_and_fetch(Some(plan))
            .await
            .unwrap()
    }

    /// Returns the current default database in this session
    #[allow(non_snake_case)]
    pub async fn currentDatabase(&mut self) -> String {
        let cat_type = Some(spark::catalog::CatType::CurrentDatabase(
            spark::CurrentDatabase {},
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::from(rel_type).clone().build_plan_root();

        self.spark_session
            .clone()
            .consume_plan_and_fetch(Some(plan))
            .await
            .unwrap()
    }

    /// Returns a list of catalogs in this session
    #[allow(non_snake_case)]
    pub async fn listCatalogs(&mut self, pattern: Option<String>) -> Vec<RecordBatch> {
        let cat_type = Some(spark::catalog::CatType::ListCatalogs(spark::ListCatalogs {
            pattern,
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::from(rel_type).clone().build_plan_root();

        self.spark_session
            .clone()
            .consume_plan(Some(plan))
            .await
            .unwrap()
    }

    /// Returns a list of databases in this session
    #[allow(non_snake_case)]
    pub async fn listDatabases(&mut self, pattern: Option<String>) -> Vec<RecordBatch> {
        let cat_type = Some(spark::catalog::CatType::ListDatabases(
            spark::ListDatabases { pattern },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::from(rel_type).clone().build_plan_root();

        self.spark_session
            .clone()
            .consume_plan(Some(plan))
            .await
            .unwrap()
    }

    /// Returns a list of tables/views in the specific database
    #[allow(non_snake_case)]
    pub async fn listTables(
        &mut self,
        dbName: Option<String>,
        pattern: Option<String>,
    ) -> Vec<RecordBatch> {
        let cat_type = Some(spark::catalog::CatType::ListTables(spark::ListTables {
            db_name: dbName,
            pattern,
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::from(rel_type).clone().build_plan_root();

        self.spark_session
            .clone()
            .consume_plan(Some(plan))
            .await
            .unwrap()
    }

    /// Returns a list of columns for the given tables/views in the specific database
    #[allow(non_snake_case)]
    pub async fn listColumns(
        &mut self,
        tableName: String,
        dbName: Option<String>,
    ) -> Vec<RecordBatch> {
        let cat_type = Some(spark::catalog::CatType::ListColumns(spark::ListColumns {
            table_name: tableName,
            db_name: dbName,
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::from(rel_type).clone().build_plan_root();

        self.spark_session
            .clone()
            .consume_plan(Some(plan))
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::SparkSessionBuilder;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_test";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_current_catalog() {
        let spark = setup().await;

        let value = spark.catalog().currentCatalog().await;

        assert_eq!(value, "spark_catalog".to_string())
    }

    #[tokio::test]
    async fn test_current_database() {
        let spark = setup().await;

        let value = spark.catalog().currentDatabase().await;

        assert_eq!(value, "default".to_string());
    }

    #[tokio::test]
    async fn test_list_catalogs() {
        let spark = setup().await;

        let value = spark.catalog().listCatalogs(None).await;

        assert_eq!(2, value[0].num_columns());
        assert_eq!(1, value[0].num_rows());
    }

    #[tokio::test]
    async fn test_list_databases() {
        let spark = setup().await;

        spark
            .clone()
            .sql("CREATE SCHEMA IF NOT EXISTS spark_rust")
            .await
            .unwrap();

        let value = spark.catalog().listDatabases(None).await;

        assert_eq!(4, value[0].num_columns());
        assert_eq!(2, value[0].num_rows());
    }

    #[tokio::test]
    async fn test_list_databases_pattern() {
        let spark = setup().await;

        spark
            .clone()
            .sql("CREATE SCHEMA IF NOT EXISTS spark_rust")
            .await
            .unwrap();

        let value = spark
            .catalog()
            .listDatabases(Some("*rust".to_string()))
            .await;

        assert_eq!(4, value[0].num_columns());
        assert_eq!(1, value[0].num_rows());
    }
}
