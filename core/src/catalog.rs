//! Spark Catalog representation through which the user may create, drop, alter or query underlying databases, tables, functions, etc.

use arrow::array::RecordBatch;

use crate::errors::SparkError;
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
    pub async fn listCatalogs(self) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListCatalogs(
            spark::ListCatalogs {},
        ));

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
    pub async fn listDatabases(self) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListDatabases(
            spark::ListDatabases {},
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

    #[allow(non_snake_case)]
    pub async fn databaseExists(self, dbName: &str) -> Result<bool, SparkError> {
        let cat_type = Some(spark::catalog::CatType::DatabaseExists(
            spark::DatabaseExists {
                db_name: dbName.to_string(),
            },
        ));

        let rel_type = spark::relation::RelType::Catalog(spark::Catalog { cat_type });

        let plan = LogicalPlanBuilder::plan_root(LogicalPlanBuilder::from(rel_type));

        let record = self.spark_session.client().to_arrow(plan).await?;

        Catalog::arrow_to_bool(record)
    }

    /// Returns a list of tables/views in the specific database
    #[allow(non_snake_case)]
    pub async fn listTables(self, dbName: Option<&str>) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListTables(spark::ListTables {
            db_name: dbName.map(|db| db.to_owned()),
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
    pub async fn listFunctions(self, dbName: Option<&str>) -> Result<RecordBatch, SparkError> {
        let cat_type = Some(spark::catalog::CatType::ListFunctions(
            spark::ListFunctions {
                db_name: dbName.map(|val| val.to_owned()),
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
    pub async fn cacheTable(self, tableName: &str) -> Result<(), SparkError> {
        let cat_type = Some(spark::catalog::CatType::CacheTable(spark::CacheTable {
            table_name: tableName.to_string(),
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

        let connection = "sc://127.0.0.1:15002/;user_id=rust_catalog";

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
    async fn test_set_current_catalog() -> Result<(), SparkError> {
        let spark = setup().await;

        spark.catalog().setCurrentCatalog("spark_catalog").await?;

        assert!(true);
        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn test_set_current_catalog_panic() -> () {
        let spark = setup().await;

        spark
            .catalog()
            .setCurrentCatalog("not_a_real_catalog")
            .await
            .unwrap();

        ()
    }

    #[tokio::test]
    async fn test_list_catalogs() -> Result<(), SparkError> {
        let spark = setup().await;

        let value = spark.catalog().listCatalogs().await?;

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
    async fn test_set_current_database() -> Result<(), SparkError> {
        let spark = setup().await;

        spark.clone().sql("CREATE SCHEMA current_db").await?;

        spark
            .clone()
            .catalog()
            .setCurrentDatabase("current_db")
            .await?;

        assert!(true);

        spark.clone().sql("DROP SCHEMA current_db").await?;

        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn test_set_current_database_panic() -> () {
        let spark = setup().await;

        spark
            .catalog()
            .setCurrentCatalog("not_a_real_db")
            .await
            .unwrap();

        ()
    }

    #[tokio::test]
    async fn test_get_database() -> Result<(), SparkError> {
        let spark = setup().await;

        spark.clone().sql("CREATE SCHEMA get_db").await?;

        let res = spark.clone().catalog().getDatabase("get_db").await?;

        assert_eq!(res.num_rows(), 1);

        spark.clone().sql("DROP SCHEMA get_db").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_database_exists() -> Result<(), SparkError> {
        let spark = setup().await;

        let res = spark.clone().catalog().databaseExists("default").await?;

        assert!(res);

        let res = spark.clone().catalog().databaseExists("not_real").await?;

        assert!(!res);
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
    async fn test_list_columns() -> Result<(), SparkError> {
        let spark = setup().await;

        spark.clone().sql("DROP TABLE IF EXISTS tmp_table").await?;

        spark
            .clone()
            .sql("CREATE TABLE tmp_table (name STRING, age INT) using parquet")
            .await?;

        let res = spark
            .clone()
            .catalog()
            .listColumns("tmp_table", None)
            .await?;

        assert_eq!(res.num_rows(), 2);

        spark.clone().sql("DROP TABLE IF EXISTS tmp_table").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_drop_view() -> Result<(), SparkError> {
        let spark = setup().await;

        spark
            .clone()
            .range(None, 2, 1, Some(1))
            .createOrReplaceGlobalTempView("tmp_view")
            .await?;

        let res = spark
            .clone()
            .catalog()
            .dropGlobalTempView("tmp_view")
            .await?;

        assert!(res);

        spark
            .clone()
            .range(None, 2, 1, Some(1))
            .createOrReplaceTempView("tmp_view")
            .await?;

        let res = spark.catalog().dropTempView("tmp_view").await?;

        assert!(res);

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_table() -> Result<(), SparkError> {
        let spark = setup().await;

        spark
            .clone()
            .sql("CREATE TABLE cache_table (name STRING, age INT) using parquet")
            .await?;

        spark.clone().catalog().cacheTable("cache_table").await?;

        let res = spark.clone().catalog().isCached("cache_table").await?;

        assert!(res);

        spark.clone().catalog().uncacheTable("cache_table").await?;

        let res = spark.clone().catalog().isCached("cache_table").await?;

        assert!(!res);

        spark.sql("DROP TABLE cache_table").await?;
        Ok(())
    }
}
