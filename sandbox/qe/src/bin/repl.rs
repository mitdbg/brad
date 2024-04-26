use arrow::util::pretty;
use clap::Parser;
use datafusion::error::DataFusionError;
use brad_qe::DB;
use brad_qe::dataset_gen::DatasetGenerators;
use rustyline::{error::ReadlineError, Editor};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

const HISTORY_FILE: &str = ".brad_qe_repl_history";

enum Command {
    Help,
    ListTables,
    ShowSchema,
    RegisterCsv,
    RegisterParquet,
    RunQuery,
    Generate,
}

impl FromStr for Command {
    type Err = ();
    fn from_str(candidate: &str) -> Result<Command, ()> {
        match candidate {
            ".help" => Ok(Command::Help),
            ".tables" => Ok(Command::ListTables),
            ".schema" => Ok(Command::ShowSchema),
            ".regcsv" => Ok(Command::RegisterCsv),
            ".regparquet" => Ok(Command::RegisterParquet),
            ".run" => Ok(Command::RunQuery),
            ".generate" => Ok(Command::Generate),
            _ => Err(()),
        }
    }
}

async fn handle_command(line: &str, db: &mut DB) -> Result<(), DataFusionError> {
    let args = line.split(' ').collect::<Vec<_>>();
    let command = match Command::from_str(args[0]) {
        Ok(cmd) => cmd,
        Err(_) => {
            println!(
                "ERROR: Unrecognized command '{}'. Type .help for help.",
                args[0]
            );
            return Ok(());
        }
    };

    match command {
        Command::Help => {
            println!("Available commands:");
            println!(".help\t\t\tPrint this help message.");
            println!(".tables\t\t\tList the registered tables.");
            println!(".schema <table>\t\tPrint the schema for a table.");
            println!(".regcsv <path>\t\tRegister a CSV file as a table.");
            println!(".regparquet <path>\tRegister a Parquet file as a table.");
            println!(".run <sql>\tRun a given query.");
            println!(".generate <name> <scale factor> [<seed>]\tPopulate the DB using a dataset generator.");
            println!("Hit Ctrl-D to exit.");
        }

        Command::ListTables => {
            let tables = db.get_table_names();
            if !tables.is_empty() {
                tables
                    .iter()
                    .for_each(|table_name| println!("{}", table_name));
            } else {
                println!("There are no registered tables.");
            }
        }

        Command::ShowSchema => {
            if args.len() < 2 {
                println!("ERROR: Specify a table name when running '.schema'.");
                return Ok(());
            }
            let table_name = args[1];
            if let Some(schema) = db.get_schema_for_table(table_name).await {
                println!("{:#?}", schema);
            } else {
                println!("ERROR: Table '{}' does not exist.", table_name);
            }
        }

        Command::RegisterCsv => {
            if args.len() < 2 {
                println!("ERROR: Specify a file path when running '.regcsv'.");
                return Ok(());
            }
            let csv_path = args[1];
            let num_added = db.register_csv(PathBuf::from(csv_path)).await?;
            println!("Registered {} new table(s).", num_added);
        }

        Command::RegisterParquet => {
            if args.len() < 2 {
                println!("ERROR: Specify a file path when running '.regparquet'.");
                return Ok(());
            }
            let parquet_path = args[1];
            let num_added = db.register_parquet(PathBuf::from(parquet_path)).await?;
            println!("Registered {} new table(s).", num_added);
        }

        Command::RunQuery => {
            if args.len() < 2 {
                println!("ERROR: Need to provide a query.");
                return Ok(());
            }
            let query = args[1..].join(" ");
            let start = Instant::now();
            let res = db.execute(&query).await?;
            let elapsed_time = start.elapsed();
            pretty::print_batches(&res)?;
            println!("(Ran for {:.2?})", elapsed_time);
        }

        Command::Generate => {
            if args.len() < 3 {
                println!(
                    "ERROR: Specify a generator name and scale factor when using '.generate'."
                );
                return Ok(());
            }
            let generator_name = args[1];
            let scale_factor = args[2].parse::<u32>();
            let seed = if args.len() >= 4 {
                args[3].parse::<u32>()
            } else {
                Ok(42)
            };
            let (scale_factor, seed) = match (scale_factor, seed) {
                (Ok(sf), Ok(sd)) => (sf, sd),
                _ => {
                    println!("ERROR: The scale factor and seed must be unsigned integers.");
                    return Ok(());
                }
            };
            if let Some(generator) = DatasetGenerators::create_from_name(generator_name) {
                let start = Instant::now();
                db.populate_using_generator(generator, scale_factor, seed)?;
                let elapsed_time = start.elapsed();
                println!("Done. (Ran for {:.2?})", elapsed_time);
            } else {
                println!("ERROR: Generator '{}' does not exist.", generator_name);
            }
        }
    };

    Ok(())
}

async fn repl_main(db: &mut DB) -> Result<(), ReadlineError> {
    let mut repl = Editor::<()>::new()?;
    if let Err(err) = repl.load_history(HISTORY_FILE) {
        // Ignore I/O errors.
        match err {
            ReadlineError::Io(_) => (),
            _ => return Err(err),
        }
    }
    loop {
        println!();
        let line = repl.readline(">> ");
        match line {
            Ok(line) => {
                repl.add_history_entry(line.as_str());
                if line.starts_with('.') {
                    if let Err(err) = handle_command(&line, db).await {
                        println!("ERROR: {:?}", err);
                    }
                } else {
                    println!("ERROR: SQL statements are not yet supported. Type .help for help.");
                }
            }
            Err(ReadlineError::Interrupted) => {
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => return Err(err),
        }
    }
    repl.save_history(HISTORY_FILE)?;
    Ok(())
}

#[derive(Parser)]
struct CliArgs {
    /// Used to specify a data source to register. Each CSV file represents a
    /// table. The table name will be its file name (without the .csv
    /// extension). Use this flag multiple times to specify multiple CSV files.
    #[clap(long)]
    csv_file: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = CliArgs::parse();
    println!("BRAD Query Executor REPL");
    println!("Type .help and hit enter for help. Hit Ctrl-D to exit.");
    let mut db = DB::new();

    let num_registered_tables = db.register_csvs(args.csv_file, None).await?;
    if num_registered_tables > 0 {
        println!("Registered {} table(s).", num_registered_tables);
    }

    if let Err(err) = repl_main(&mut db).await {
        println!("REPL Error: {:?}", err);
    }

    Ok(())
}
