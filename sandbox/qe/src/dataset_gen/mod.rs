use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Employees/departments dataset.
/// Also see `cirrus::hardcoded_system_plans::emps_depts`.
mod emps_depts;

pub trait DatasetGenerator {
    /// Returns the tables in the dataset. Each tuple in the returned vector
    /// contains the table's name and their schema.
    fn schemas(&self) -> Vec<(String, SchemaRef)>;

    /// Generate the dataset using the provided scale factor and pseudo-random
    /// number generator seed.
    fn generate(&self, scale_factor: u32, seed: u32) -> Vec<(String, Vec<RecordBatch>)>;
}

/// The dataset generators that have been hardcoded.
pub enum DatasetGenerators {
    EmpsDepts,
}

impl DatasetGenerators {
    pub fn create(dataset: DatasetGenerators) -> Arc<impl DatasetGenerator> {
        match dataset {
            DatasetGenerators::EmpsDepts => Arc::new(emps_depts::EmpsDeptsGenerator::new()),
        }
    }

    pub fn create_from_name(name: &str) -> Option<Arc<impl DatasetGenerator>> {
        match name {
            "emps_depts" => Some(Self::create(Self::EmpsDepts)),
            _ => None,
        }
    }
}
