use arrow::util::pretty;
use brad_qe::rules::AddCustomFilter;
use brad_qe::DB;
use clap::Parser;
use datafusion::error::DataFusionError;
use datafusion::execution::options::CsvReadOptions;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
struct CliArgs {
    /// Path to a directory containing TPC-H files in text format.
    #[clap(long)]
    tpch_txt_dir: PathBuf,

    /// Data delimiter used in the data files.
    /// Default value is `b'|'`
    #[clap(long)]
    delimiter: Option<u8>,

    /// Action to take.
    #[clap(long)]
    action: Option<String>,

    // Number of times to run the experiment.
    #[clap(long)]
    repetitions: Option<u32>,
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

async fn run_tpch_queries(db: &DB, repetitions: u32) -> Result<(), DataFusionError> {
    let mut writer = csv::Writer::from_writer(io::stdout());
    writer
        .write_record(&["query", "avg_run_time_ms"])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    for q in 1..23 {
        let path = format!("./tpch/queries/{q}.sql");
        let query = fs::read_to_string(path)?;
        let mut total_time = 0;
        // Discard first run to allow for caching/initialization overhead
        run_timed_query(&db, &query).await?;
        for _ in 0..repetitions {
            let rt = run_timed_query(&db, &query).await?;
            total_time += &rt.as_millis();
        }
        let avg_time = total_time / (repetitions as u128);
        writer
            .write_record(&[&q.to_string(), &avg_time.to_string()])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }
    writer.flush()?;
    Ok(())
}

async fn run_timed_query(db: &DB, query: &String) -> Result<Duration, DataFusionError> {
    let start = Instant::now();
    db.execute(query).await?;
    Ok(start.elapsed())
}

async fn load_data(db: &DB, args: &CliArgs) -> Result<(), DataFusionError> {
    let files = get_files(&args.tpch_txt_dir);
    eprintln!("Detected data files:");
    for f in &files {
        eprintln!("{}", f.display());
    }

    eprintln!("\nLoading data...");
    let delim = args.delimiter.unwrap_or(b',');
    let csv_options = CsvReadOptions::new()
        .has_header(true)
        .delimiter(delim)
        .file_extension(".csv");
    db.register_csvs_as_memtables(files, Some(csv_options), true)
        .await?;
    eprintln!("Done!\n");
    Ok(())
}

const QUERY_SIMPLE: &str =
    "SELECT o_orderkey FROM orders WHERE o_orderdate < date '1995-03-15' LIMIT 1;";

fn qs_inject(node: &Arc<dyn ExecutionPlan>) -> bool {
    let n = node.clone();
    // Inject just after the deepest `FilterExec`.
    n.as_any().downcast_ref::<FilterExec>().is_some()
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = CliArgs::parse();
    let db = DB::new();

    load_data(&db, &args).await?;

    match args.action {
        Some(ref s) if s == "tpch" => {
            let repetitions = args.repetitions.unwrap_or(1);
            run_tpch_queries(&db, repetitions).await?;
        }
        Some(ref s) if s == "qs_filter" => {
            let query = String::from(QUERY_SIMPLE);
            let orig_physical_plan = db.to_physical_plan(&query).await?;
            let dpp = displayable(orig_physical_plan.as_ref());
            eprintln!("\nOriginal plan\n{}", dpp.indent(false));

            let add_custom_filter = Arc::new(AddCustomFilter::new(qs_inject));
            let new_physical_plan = db
                .to_physical_plan_with_custom_rules(&query, vec![add_custom_filter])
                .await?;

            let dpp2 = displayable(new_physical_plan.as_ref());
            eprintln!("\nAltered plan\n{}", dpp2.indent(false));

            let start = Instant::now();
            let results = db.execute_physical_plan(new_physical_plan).await?;
            let elapsed_time = start.elapsed();
            pretty::print_batches(&results)?;
            eprintln!("Ran for {:.2?}", elapsed_time);
        }
        _ => (),
    }

    Ok(())
}
