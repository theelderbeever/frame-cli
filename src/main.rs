mod agg;
mod select;
mod to;
mod utils;

use std::{fs::create_dir, path::PathBuf};

use clap::{Parser, Subcommand};
use dirs::home_dir;
use duckdb::{Connection, Result};
use log::debug;
use regex::Regex;

use agg::AggCommand;
use select::SelectCommand;
use utils::send_display_query;

fn default_db_path() -> PathBuf {
    let frame_dir = home_dir().unwrap().join(".frame");
    if !frame_dir.exists() {
        create_dir(&frame_dir).unwrap()
    }
    frame_dir.join("frame.db")
}

fn main() -> Result<()> {
    env_logger::init();

    // let conn = Connection::open_in_memory()?;
    let args = DfCli::parse();
    debug!("{:#?}", args);
    let conn = Connection::open(args.db)?;
    let re = Regex::new(r"\.(csv|parquet|json|ndjson)").unwrap();
    let ext = re
        .find(
            args.from
                .file_name()
                .ok_or(duckdb::Error::InvalidPath(args.from.clone()))?
                .to_str()
                .unwrap(),
        )
        .map(|m| m.as_str());
    debug!("From format is {:#?}", ext);
    let from = match ext {
        Some(".parquet") => format!("READ_PARQUET('{}')", args.from.to_str().unwrap()),
        Some(".csv") => format!("READ_CSV_AUTO('{}')", args.from.to_str().unwrap()),
        Some(".json") => format!("READ_JSON_AUTO('{}')", args.from.to_str().unwrap()),
        Some(".ndjson") => format!("READ_NDJSON_AUTO('{}')", args.from.to_str().unwrap()),
        _ => args.from.to_str().unwrap().to_string(),
    };
    match args.command {
        Commands::Describe => send_display_query(conn, format!("DESCRIBE SELECT * FROM {}", from))?,
        Commands::Select(select) => select.run(conn, from)?,
        Commands::Agg(agg) => agg.run(conn, from)?,
    };
    Ok(())
}

fn print_version() -> &'static str {
    Box::leak(format!("v{}", env!("CARGO_PKG_VERSION")).into())
}

#[derive(Debug, Parser)]
#[command(name = "frame")]
#[command(version = print_version(), about = "Dataframe cli for operating on data files.", long_about = None)]
struct DfCli {
    #[arg(long, default_value=default_db_path().into_os_string(), help = "Path to persistent database. Unnecessary for strictly file based ops.")]
    db: PathBuf,
    #[arg(help = "Path to file or glob pattern.")]
    from: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Commands {
    Describe,
    Select(SelectCommand),
    Agg(AggCommand),
}
