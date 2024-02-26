use arrow::util::pretty;
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

async fn _run_query_and_print_results(db: &DB, query: &String) -> Result<(), DataFusionError> {
    let results = db.execute(query).await?;
    pretty::print_batches(&results)?;
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

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = CliArgs::parse();
    let db = DB::new();

    let files = get_files(&args.tpch_txt_dir);
    println!("Detected data files:");
    for f in &files {
        println!("{}", f.display());
    }

    println!("\nLoading data...");
    let csv_options = CsvReadOptions::new()
        .has_header(true)
        .delimiter(b'|')
        .file_extension(".tbl");
    db.register_csvs(files, Some(csv_options)).await?;
    println!("Done!\n");

    // Print out tables.
    // let tables = db.get_table_names();
    // for t in &tables {
    //     println!("{}", t);
    //     if let Some(schema) = db.get_schema_for_table(t) {
    //         println!("{:#?}", schema);
    //     }
    // }

    // run_query_and_print_results(&db, &"SELECT * FROM region".to_string()).await?;
    // run_query_and_print_results(&db, &QUERY_3.to_string()).await?;

    let q3 = String::from(QUERY_3);
    let logical_plan = db.to_logical_plan(&q3)?;
    println!("{:#?}", logical_plan);

    // let physical_plan = db.to_physical_plan(&q3).await?;
    // println!("\n{:#?}", physical_plan);
    Ok(())
}
