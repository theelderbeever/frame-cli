use std::path::PathBuf;

use clap::{Args, ValueEnum};
use duckdb::{Connection, Result};

use crate::utils::send_display_query;

#[derive(ValueEnum, Clone, Debug)]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Zstd,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum FileFormat {
    Json,
    NdJson,
    Csv,
    Parquet,
}

#[derive(Debug, Args)]
#[command(
    args_conflicts_with_subcommands = false,
    arg_required_else_help = true,
    about = "To columns from dataset."
)]
pub struct ToCommand {
    #[arg(help = "Columns to group on. Must have at least one agg column in to.")]
    to: PathBuf,
    #[arg(
        short,
        long,
        value_enum,
        help = "Path to write result to. Displays to stdout if not provided"
    )]
    compression: Option<Compression>,
    #[arg(
        short,
        long,
        value_enum,
        help = "Path to write result to. Displays to stdout if not provided"
    )]
    format: Option<FileFormat>,
    #[arg(
        short,
        long,
        value_delimiter = ' ',
        num_args = 0..,
        help = "Path to write result to. Displays to stdout if not provided"
    )]
    partition_by: Option<Vec<String>>,
    #[arg(
        short,
        long,
        help = "Path to write result to. Displays to stdout if not provided"
    )]
    threads: Option<usize>,
}

impl ToCommand {
    pub fn run(&self, conn: Connection, from: String) -> Result<()> {
        // send_display_query(conn, query)?;
        Ok(())
    }
}
