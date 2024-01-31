use std::vec;

use clap::{Args, Subcommand};
use duckdb::{Connection, Result};

use crate::utils::{join_cols, send_display_query};

#[derive(Debug, Args)]
#[command(
    args_conflicts_with_subcommands = false,
    arg_required_else_help = true,
    about = "Select columns from dataset."
)]
pub struct SelectCommand {
    #[arg()]
    columns: Vec<String>,
    #[arg(
        short,
        long,
        default_value_t = 10,
        help = "Limit rows to display. Use -1 to return all rows."
    )]
    limit: i128,
    #[command(subcommand)]
    where_: Option<Where>,
    #[arg(
        short,
        long,
        help = "Path to write result to. Displays to stdout if not provided"
    )]
    output: Option<Vec<String>>,
}

impl SelectCommand {
    pub fn run(&self, conn: Connection, from: String) -> Result<()> {
        // let query_parts: Vec<String> = vec![String::from("SELECT")];

        // query_parts.push(join_cols(&self.columns, None));

        // query_parts.push(format!("FROM {}", from));

        // if self.limit.is_positive() {
        //     query_parts.push(format!("LIMIT {}", self.limit));
        // };

        // send_display_query(conn, query)?;
        Ok(())
    }
}

#[derive(Debug, Args)]
#[command(
    args_conflicts_with_subcommands = false,
    arg_required_else_help = true,
    about = "Select columns from dataset."
)]
pub struct WhereCommand {
    #[arg()]
    where_: String,
    #[arg(short, long)]
    and: Option<Vec<String>>,
    #[arg(short, long)]
    or: Option<Vec<String>>,
}

impl WhereCommand {
    pub fn build(&self) -> String {
        let mut clause_parts = vec!["WHERE", &self.where_];
        if let Some(conditions) = &self.and {
            for condition in conditions.iter() {
                clause_parts.push("AND");
                clause_parts.push(condition);
            }

            if let Some(conditions) = &self.or {
                for condition in conditions.iter() {
                    clause_parts.push("OR");
                    clause_parts.push(condition);
                }
            }
        }

        clause_parts.join(" ")
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Where {
    Where(WhereCommand),
}
