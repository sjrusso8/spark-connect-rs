//! Spark Catalog representation through which the user may create, drop, alter or query underlying databases, tables, functions, etc.

use arrow::array::RecordBatch;

use crate::errors::SparkError;
use crate::plan::LogicalPlanBuilder;
use crate::session::SparkSession;
use crate::spark;
use crate::storage::StorageLevel;

#[derive(Debug, Clone)]
pub struct Catalog {
    spark_session: SparkSession,
}

impl Catalog {
    pub fn new(spark_session: SparkSession) -> Self {
        Self { spark_session }
    }

    fn arrow_to_bool(record: RecordBatch) -> Result<bool, SparkError> {
        let col = record.column(0);

        let data: &arrow::array::BooleanArray = match col.data_type() {
            arrow::datatypes::DataType::Boolean => col.as_any().downcast_ref().unwrap(),
            _ => unimplemented!("only Boolean data types are currently handled currently."),
        };

        Ok(data.value(0))
    }

    /// Returns the current default catalog in this session
    #[allow(non_snake_case)]
    pub async fn currentCatalog(self) -> Result<String, SparkError> {
        let cat_type = Some(spark::catalog::CatType::CurrentCatalog(
            spark::CurrentCatalog {},
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_first_value(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn setCurrentCatalog(self, catalogName: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::SetCurrentCatalog(
            spark::SetCurrentCatalog {
                catalog_name: catalogName.to_string(),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    /// Returns a list of catalogs in this session
    #[allow(non_snake_case)]
    pub async fn listCatalogs(self, pattern: Option<&str>) -> Result<RecordBatch, SparkError> {
        let pattern = pattern.map(|val| val.to_owned());

        let cat_type = Some(spark::catalog::CatType::ListCatalogs(spark::ListCatalogs {
            pattern,
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    /// Returns the current default database in this session
    #[allow(non_snake_case)]
    pub async fn currentDatabase(self) -> Result<String, SparkError> {
        let cat_type = Some(spark::catalog::CatType::CurrentDatabase(
            spark::CurrentDatabase {},
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_first_value(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn setCurrentDatabase(self, dbName: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::SetCurrentDatabase(
            spark::SetCurrentDatabase {
                db_name: dbName.to_string(),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    /// Returns a list of databases in this session
    #[allow(non_snake_case)]
    pub async fn listDatabases(self, pattern: Option<&str>) -> Result<RecordBatch, SparkError> {
        let pattern = pattern.map(|val| val.to_owned());

        let cat_type = Some(spark::catalog::CatType::ListDatabases(
            spark::ListDatabases { pattern },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn getDatabase(self, dbName: &str) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::GetDatabase(spark::GetDatabase {
            db_name: dbName.to_string(),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    /// Returns a list of tables/views in the specific database
    #[allow(non_snake_case)]
    pub async fn listTables(
        self,
        dbName: Option<&str>,
        pattern: Option<&str>,
    ) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListTables(spark::ListTables {
            db_name: dbName.map(|db| db.to_owned()),
            pattern: pattern.map(|val| val.to_owned()),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn getTable(self, tableName: &str) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::GetTable(spark::GetTable {
            table_name: tableName.to_string(),
            db_name: None,
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn listFunctions(
        self,
        dbName: Option<&str>,
        pattern: Option<&str>,
    ) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListFunctions(
            spark::ListFunctions {
                db_name: dbName.map(|val| val.to_owned()),
                pattern: pattern.map(|val| val.to_owned()),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn functionExists(
        self,
        functionName: &str,
        dbName: Option<&str>,
    ) -> Result<bool, SparkError> {
        let cat_type = Some(spark::catalog::CatType::FunctionExists(
            spark::FunctionExists {
                function_name: functionName.to_string(),
                db_name: dbName.map(|val| val.to_owned()),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        let record = self.spark_session.client().to_arrow(plan).await?;

        Catalog::arrow_to_bool(record)
    }

    #[allow(non_snake_case)]
    pub async fn getFunction(self, functionName: &str) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::GetFunction(spark::GetFunction {
            function_name: functionName.to_string(),
            db_name: None,
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    /// Returns a list of columns for the given tables/views in the specific database
    #[allow(non_snake_case)]
    pub async fn listColumns(
        self,
        tableName: &str,
        dbName: Option<&str>,
    ) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListColumns(spark::ListColumns {
            table_name: tableName.to_owned(),
            db_name: dbName.map(|val| val.to_owned()),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().to_arrow(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn tableExists(
        self,
        tableName: &str,
        dbName: Option<&str>,
    ) -> Result<bool, SparkError> {
        let cat_type = Some(spark::catalog::CatType::TableExists(spark::TableExists {
            table_name: tableName.to_string(),
            db_name: dbName.map(|val| val.to_owned()),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        let record = self.spark_session.client().to_arrow(plan).await?;

        Catalog::arrow_to_bool(record)
    }

    #[allow(non_snake_case)]
    pub async fn dropTempView(self, viewName: &str) -> Result<bool, SparkError> {
        let cat_type = Some(spark::catalog::CatType::DropTempView(spark::DropTempView {
            view_name: viewName.to_string(),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        let record = self.spark_session.client().to_arrow(plan).await?;

        Catalog::arrow_to_bool(record)
    }

    #[allow(non_snake_case)]
    pub async fn dropGlobalTempView(self, viewName: &str) -> Result<bool, SparkError> {
        let cat_type = Some(spark::catalog::CatType::DropGlobalTempView(
            spark::DropGlobalTempView {
                view_name: viewName.to_string(),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        let record = self.spark_session.client().to_arrow(plan).await?;

        Catalog::arrow_to_bool(record)
    }

    #[allow(non_snake_case)]
    pub async fn isCached(self, tableName: &str) -> Result<bool, SparkError> {
        let cat_type = Some(spark::catalog::CatType::IsCached(spark::IsCached {
            table_name: tableName.to_string(),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        let record = self.spark_session.client().to_arrow(plan).await?;

        Catalog::arrow_to_bool(record)
    }

    #[allow(non_snake_case)]
    pub async fn cachedTable(
        self,
        tableName: &str,
        storageLevel: Option<StorageLevel>,
    ) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::CacheTable(spark::CacheTable {
            table_name: tableName.to_string(),
            storage_level: storageLevel.map(|val| val.to_owned().into()),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn uncacheTable(self, tableName: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::UncacheTable(spark::UncacheTable {
            table_name: tableName.to_string(),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn clearCache(self) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::ClearCache(spark::ClearCache {}));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn refreshTable(self, tableName: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::RefreshTable(spark::RefreshTable {
            table_name: tableName.to_string(),
        }));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn recoverPartitions(self, tableName: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::RecoverPartitions(
            spark::RecoverPartitions {
                table_name: tableName.to_string(),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }

    #[allow(non_snake_case)]
    pub async fn refreshByPath(self, path: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::RefreshByPath(
            spark::RefreshByPath {
                path: path.to_string(),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        self.spark_session.client().execute_command(plan).await
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::errors::SparkError;
    use crate::SparkSessionBuilder;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_catalog;session_id=f93c9562-cb73-473c-add4-c73a236e50dc";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_current_catalog() -> Result<(), SparkError> {
        let spark = setup().await;

        let value = spark.catalog().currentCatalog().await?;

        assert_eq!(value, "spark_catalog".to_string());
        Ok(())
    }
    #[tokio::test]
    async fn test_list_catalogs() -> Result<(), SparkError> {
        let spark = setup().await;

        let value = spark.catalog().listCatalogs(None).await?;

        assert_eq!(2, value.num_columns());
        assert_eq!(1, value.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_current_database() -> Result<(), SparkError> {
        let spark = setup().await;

        let value = spark.catalog().currentDatabase().await?;

        assert_eq!(value, "default".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn test_list_databases() -> Result<(), SparkError> {
        let spark = setup().await;

        spark
            .clone()
            .sql("CREATE SCHEMA IF NOT EXISTS spark_rust")
            .await
            .unwrap();

        let value = spark.catalog().listDatabases(None).await?;

        assert_eq!(4, value.num_columns());
        assert_eq!(2, value.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_database() -> Result<(), SparkError> {
        let spark = setup().await;

        spark
            .clone()
            .sql("CREATE SCHEMA IF NOT EXISTS spark_rust")
            .await?;

        let values = spark.catalog().getDatabase("spark_rust").await?;

        println!("{:?}", values);

        assert_eq!(1, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_function_exists() -> Result<(), SparkError> {
        let spark = setup().await;

        let res = spark.catalog().functionExists("len", None).await?;

        assert!(res);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_databases_pattern() -> Result<(), SparkError> {
        let spark = setup().await;

        spark
            .clone()
            .sql("CREATE SCHEMA IF NOT EXISTS spark_rust")
            .await
            .unwrap();

        let value = spark.catalog().listDatabases(Some("*rust")).await?;

        assert_eq!(4, value.num_columns());
        assert_eq!(1, value.num_rows());

        Ok(())
    }
}
