use brad_qe::DB;
use clap::Parser;
use datafusion::error::DataFusionError;
use datafusion::execution::options::CsvReadOptions;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
struct CliArgs {
    /// Path to a directory containing TPC-H files in text format.
    #[clap(long)]
    tpch_txt_dir: PathBuf,

    /// Data delimiter used in the data files.
    #[clap(long)]
    delimiter: Option<char>,
}

fn get_files(data_dir: &PathBuf) -> Vec<PathBuf> {
    if let Ok(entries) = fs::read_dir(data_dir) {
        let files: Vec<PathBuf> = entries
            .filter_map(|entry| {
                if let Ok(entry) = entry {
                    Some(entry.path())
                } else {
                    None
                }
            })
            .collect();
        files
    } else {
        vec![]
    }
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = CliArgs::parse();
    let db = DB::new();

    let files = get_files(&args.tpch_txt_dir);
    for f in &files {
        println!("{}", f.display());
    }
    let csv_options = CsvReadOptions::new()
        .has_header(false)
        .delimiter(b'|')
        .file_extension(".tbl");
    db.register_csvs(files, Some(csv_options)).await?;

    // Print out tables.
    let tables = db.get_table_names();
    for t in &tables {
        println!("{}", t);
        if let Some(schema) = db.get_schema_for_table(t) {
            println!("{:#?}", schema);
        }
    }

    Ok(())
}
