use super::DatasetGenerator;
use arrow::array::{ArrayRef, GenericStringBuilder, PrimitiveBuilder};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::sync::Arc;

pub struct EmpsDeptsGenerator {
    tables: HashMap<String, SchemaRef>,
}

impl EmpsDeptsGenerator {
    pub fn new() -> Self {
        // Employees table.
        let columns = vec![
            Field::new("e_id", DataType::Int64, false),
            Field::new("e_name", DataType::Utf8, false),
            // Days since January 1, 1970.
            Field::new("e_hire_date", DataType::Int64, false),
            Field::new("e_salary", DataType::Int64, false),
            Field::new("e_d_id", DataType::Int64, false),
        ];
        let emps_table = ("employees".to_string(), Arc::new(Schema::new(columns)));
        let tables = vec![emps_table];
        Self {
            tables: tables.into_iter().collect(),
        }
    }

    fn schema_for(&self, table: &str) -> SchemaRef {
        self.tables.get(table).unwrap().clone()
    }

    fn generate_impl(
        &self,
        scale_factor: u32,
        seed: u32,
    ) -> Result<Vec<(String, Vec<RecordBatch>)>, ArrowError> {
        let num_employees = (scale_factor * 10000) as usize;
        let num_depts = scale_factor * 100;

        let emps_schema = self.schema_for("employees");
        let mut prng = SmallRng::seed_from_u64(seed.into());

        let hire_date_range = 0..((3650 * 6) as i64);
        let salary_range = 0..1000_i64;
        let d_id_range = 0..(num_depts as i64);

        let mut e_id_builder = PrimitiveBuilder::<Int64Type>::with_capacity(num_employees);
        let mut e_name_builder =
            GenericStringBuilder::<i32>::with_capacity(num_employees, num_employees * 12);
        let mut e_hire_date_builder = PrimitiveBuilder::<Int64Type>::with_capacity(num_employees);
        let mut e_salary_builder = PrimitiveBuilder::<Int64Type>::with_capacity(num_employees);
        let mut e_d_id_builder = PrimitiveBuilder::<Int64Type>::with_capacity(num_employees);

        for i in 0..num_employees {
            e_id_builder.append_value(i as i64);
            e_name_builder.append_value(format!("E{}", i));
            e_hire_date_builder.append_value(prng.gen_range(hire_date_range.clone()));
            e_salary_builder.append_value(prng.gen_range(salary_range.clone()));
            e_d_id_builder.append_value(prng.gen_range(d_id_range.clone()));
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(e_id_builder.finish()),
            Arc::new(e_name_builder.finish()),
            Arc::new(e_hire_date_builder.finish()),
            Arc::new(e_salary_builder.finish()),
            Arc::new(e_d_id_builder.finish()),
        ];

        let employees = RecordBatch::try_new(emps_schema, cols)?;
        Ok(vec![(String::from("employees"), vec![employees])])
    }
}

impl DatasetGenerator for EmpsDeptsGenerator {
    // employees(e_id INT, e_name TEXT, e_hire_date INT, e_salary INT, e_d_id INT)
    fn schemas(&self) -> Vec<(String, SchemaRef)> {
        self.tables
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<(String, SchemaRef)>>()
    }

    fn generate(&self, scale_factor: u32, seed: u32) -> Vec<(String, Vec<RecordBatch>)> {
        self.generate_impl(scale_factor, seed).unwrap()
    }
}
