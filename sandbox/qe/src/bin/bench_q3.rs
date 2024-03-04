use arrow::util::pretty;
use brad_qe::DB;
use brad_qe::rewrite::inject_tap;
use clap::Parser;
use datafusion::error::DataFusionError;
use datafusion::execution::options::CsvReadOptions;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::sync::Arc;

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

async fn run_query_and_print_results(
    db: &DB,
    query: &str,
    debug: bool,
    skip_execution: bool,
) -> Result<(), DataFusionError> {
    let query = query.to_string();
    if debug {
        let logical_plan = db.to_logical_plan(&query).await?;
        eprintln!("{:#?}", logical_plan);

        let physical_plan = db.to_physical_plan(&query).await?;
        let dpp = displayable(physical_plan.as_ref());
        eprintln!("\n{}", dpp.indent(false));
    }
    if !skip_execution {
        let start = Instant::now();
        let results = db.execute(&query).await?;
        let elapsed_time = start.elapsed();
        pretty::print_batches(&results)?;
        eprintln!("Ran for {:.2?}", elapsed_time);
    }
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
    let delim = args.delimiter.unwrap_or(b'|');
    let csv_options = CsvReadOptions::new()
        .has_header(true)
        .delimiter(delim)
        .file_extension(".tbl");
    db.register_csvs_as_memtables(files, Some(csv_options), true)
        .await?;
    eprintln!("Done!\n");
    Ok(())
}

const QUERY_3: &str = "
SELECT
	l_orderkey,
	SUM(l_extendedprice * (1 - l_discount)) AS revenue,
	o_orderdate,
	o_shippriority
FROM
	customer,
	orders,
	lineitem
WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate
LIMIT 10;
";

const QUERY_SIMPLE: &str =
    "SELECT o_orderkey FROM orders WHERE o_orderdate < date '1995-03-15' LIMIT 10;";

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

    // Print out tables.
    // let tables = db.get_table_names();
    // for t in &tables {
    //     println!("{}", t);
    //     if let Some(schema) = db.get_schema_for_table(t) {
    //         println!("{:#?}", schema);
    //     }
    // }

    match args.action {
        Some(ref s) if s == "q3" => {
            let repetitions = args.repetitions.unwrap_or(1);
            let q3 = String::from(QUERY_3);
            let mut writer = csv::Writer::from_writer(io::stdout());
            writer
                .write_record(&["action", "run_time_ms"])
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            for _ in 0..repetitions {
                let rt = run_timed_query(&db, &q3).await?;
                writer
                    .write_record(&["q3", &rt.as_millis().to_string()])
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            writer.flush()?;
        }
        Some(ref s) if s == "q3_debug" => {
            run_query_and_print_results(&db, QUERY_3, true, false).await?;
        }
        Some(ref s) if s == "qs_debug" => {
            run_query_and_print_results(&db, QUERY_SIMPLE, true, false).await?;
        }
        Some(ref s) if s == "qs_tap" => {
            let query = String::from(QUERY_SIMPLE);
            let orig_physical_plan = db.to_physical_plan(&query).await?;
            let dpp = displayable(orig_physical_plan.as_ref());
            eprintln!("\nOriginal plan\n{}", dpp.indent(false));

            let new_physical_plan = inject_tap(&orig_physical_plan, qs_inject)?;
            if let Some(npp) = new_physical_plan {
                let dpp2 = displayable(npp.as_ref());
                eprintln!("\nAltered plan\n{}", dpp2.indent(false));

                let start = Instant::now();
                let results = db.execute_physical_plan(npp).await?;
                let elapsed_time = start.elapsed();
                pretty::print_batches(&results)?;
                eprintln!("Ran for {:.2?}", elapsed_time);

            } else {
                eprintln!("\nNo modifications.");
            }
        }
        _ => (),
    }

    Ok(())
}
