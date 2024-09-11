use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableReference;
use datafusion::datasource::memory::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use futures::future;
use std::path::PathBuf;
use std::sync::Arc;

/// Encapsulates DataFusion related state.
mod datafusion_state;

/// Used to generate datasets kept in memory.
pub mod dataset_gen;
use dataset_gen::DatasetGenerator;

/// Custom DataFusion physical operators (`ExecutionPlan`s).
pub mod ops;

/// Utilities for rewriting DataFusion `ExecutionPlan`s.
pub mod rewrite;

// RadixSpline bindings. Currently not working.
// pub mod radixspline;

/// Represents an "open" IOHTAP database. Eventually, the DB should run as a
/// daemon process. For now it is just an embedded DB (similar to SQLite).
pub struct DB {
    dfusion: Arc<datafusion_state::DataFusionState>,
}

impl DB {
    pub fn new() -> Self {
        Self {
            dfusion: Arc::new(datafusion_state::DataFusionState::new()),
        }
    }

    pub async fn register_csv(&self, csv_file: PathBuf) -> Result<usize, DataFusionError> {
        self.register_csvs(vec![csv_file], None).await
    }

    pub async fn register_csvs<'a>(
        &self,
        csv_files: Vec<PathBuf>,
        options: Option<CsvReadOptions<'a>>,
    ) -> Result<usize, DataFusionError> {
        let table_paths_and_names = csv_files
            .into_iter()
            .filter_map(|path| {
                let mstr_path = path.to_str();
                let mtable_name = path.file_stem().and_then(|stem| stem.to_str());
                match (mstr_path, mtable_name) {
                    (Some(str_path), Some(table_name)) => {
                        Some((str_path.to_string(), table_name.to_string()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<(String, String)>>();
        let num_tables = table_paths_and_names.len();
        let inner_options = if let Some(inner) = options {
            inner
        } else {
            CsvReadOptions::new()
        };
        future::try_join_all(table_paths_and_names.iter().map(|(str_path, table_name)| {
            self.dfusion
                .session_context()
                .register_csv(table_name, str_path, inner_options.clone())
        }))
        .await?;
        Ok(num_tables)
    }

    pub async fn register_csvs_as_memtables<'a>(
        &self,
        csv_files: Vec<PathBuf>,
        options: Option<CsvReadOptions<'a>>,
        verbose: bool,
    ) -> Result<usize, DataFusionError> {
        let table_paths_and_names = csv_files
            .into_iter()
            .filter_map(|path| {
                let mstr_path = path.to_str();
                let mtable_name = path.file_stem().and_then(|stem| stem.to_str());
                match (mstr_path, mtable_name) {
                    (Some(str_path), Some(table_name)) => {
                        Some((str_path.to_string(), table_name.to_string()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<(String, String)>>();
        let num_tables = table_paths_and_names.len();
        let inner_options = if let Some(inner) = options {
            inner
        } else {
            CsvReadOptions::new()
        };
        let ctx = self.dfusion.session_context();
        let schema_provider = self.dfusion.schema_provider();
        for (str_path, table_name) in table_paths_and_names {
            if verbose {
                eprintln!("Registering {}...", table_name);
            }
            ctx.register_csv(&table_name, &str_path, inner_options.clone())
                .await?;
            let query = format!("SELECT * FROM {}", &table_name);
            let records = self.execute(&query).await?;
            let schema = schema_provider.table(&table_name).await.unwrap().schema();
            let table_ref = TableReference::bare(&table_name);
            ctx.deregister_table(table_ref)?;

            // Re-register it but with Arrow data instead.
            let provider = Arc::new(MemTable::try_new(schema, vec![records])?);
            schema_provider.register_table(table_name, provider)?;
        }
        Ok(num_tables)
    }

    pub async fn register_parquet(&self, parquet_file: PathBuf) -> Result<usize, DataFusionError> {
        self.register_parquets(vec![parquet_file]).await
    }

    pub async fn register_parquets(
        &self,
        parquet_files: Vec<PathBuf>,
    ) -> Result<usize, DataFusionError> {
        let table_paths_and_names = parquet_files
            .into_iter()
            .filter_map(|path| {
                let mstr_path = path.to_str();
                let mtable_name = path.file_stem().and_then(|stem| stem.to_str());
                match (mstr_path, mtable_name) {
                    (Some(str_path), Some(table_name)) => {
                        Some((str_path.to_string(), table_name.to_string()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<(String, String)>>();
        let num_tables = table_paths_and_names.len();
        future::try_join_all(table_paths_and_names.iter().map(|(str_path, table_name)| {
            self.dfusion.session_context().register_parquet(
                table_name,
                str_path,
                ParquetReadOptions {
                    ..Default::default()
                },
            )
        }))
        .await?;
        Ok(num_tables)
    }

    pub async fn execute(&self, query: &String) -> Result<Vec<RecordBatch>, DataFusionError> {
        let physical_plan = self.to_physical_plan(query).await?;
        self.dfusion.execute_datafusion_plan(physical_plan).await
    }

    pub async fn to_logical_plan(&self, query: &String) -> Result<LogicalPlan, DataFusionError> {
        let ctx = self.dfusion.session_context();
        let state = ctx.state();
        state.create_logical_plan(query).await
    }

    pub async fn to_physical_plan(
        &self,
        query: &String,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ctx = self.dfusion.session_context();
        let state = ctx.state();
        let logical_plan = state.create_logical_plan(query).await?;
        let optimized_plan = state.optimize(&logical_plan)?;
        state.create_physical_plan(&optimized_plan).await
    }

    pub async fn execute_physical_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.dfusion.execute_datafusion_plan(plan).await
    }

    /// Populates the database using data generated by the specified generator.
    pub fn populate_using_generator(
        &mut self,
        generator: Arc<impl DatasetGenerator>,
        scale_factor: u32,
        seed: u32,
    ) -> Result<(), DataFusionError> {
        let schema_provider = self.dfusion.schema_provider();
        let data = generator.generate(scale_factor, seed);
        data.into_iter()
            .map(|(name, records)| {
                let schema = records.first().unwrap().schema();
                let provider = Arc::new(MemTable::try_new(schema, vec![records])?);
                schema_provider.register_table(name, provider)?;
                Ok(())
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        Ok(())
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.dfusion.schema_provider().table_names()
    }

    pub async fn get_schema_for_table(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.dfusion
            .schema_provider()
            .table(table_name)
            .await
            .map(|tbl| tbl.schema())
    }
}

impl Default for DB {
    fn default() -> Self {
        Self::new()
    }
}
