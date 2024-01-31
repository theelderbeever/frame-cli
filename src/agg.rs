use clap::Args;
use duckdb::{Connection, Result};

use crate::utils::{join_cols, send_display_query};

#[derive(Debug, Args)]
#[command(
    args_conflicts_with_subcommands = false,
    arg_required_else_help = true,
    about = "Select columns from dataset."
)]
pub struct AggCommand {
    #[arg(help = "Columns to group the aggs by. Not necessary for all agg methods.")]
    group_by: Option<Vec<String>>,
    #[arg(short, long, num_args = 0.., value_delimiter = ' ')]
    count: Option<Vec<String>>,
    #[arg(short = 'C', long, num_args = 0.., value_delimiter = ' ')]
    count_distinct: Option<Vec<String>>, // TODO: fix the alias on count distinct
    #[arg(short, long, num_args = 0..,value_delimiter = ' ', help = "Calculates the average value for each column.")]
    avg: Option<Vec<String>>,
    #[arg(short = 'G',long, num_args = 0..,value_delimiter = ' ',help = "Calculates the geometric mean of each columns.")]
    geomean: Option<Vec<String>>,
    #[arg(short = 'L', long, num_args = 0.., value_delimiter = ' ', help = "Returns a LIST of the columns for each argument. Format is a space delimited list of comma separated columns. Example: '--list c1,c2,c3 c4,c5'")]
    list: Option<Vec<String>>,
    #[arg(short = 'H', long, num_args = 0.., value_delimiter = ' ', help = "Returns a LIST of STRUCTs with the fields bucket and count.")]
    histogram: Option<Vec<String>>,
    #[arg(short = 'M', long, num_args = 0.., value_delimiter = ' ', help = "Finds the max value of each column.")]
    max: Option<Vec<String>>,
    #[arg(short = 'm', long, num_args = 0.., value_delimiter = ' ', help = "Finds the min value of each column.")]
    min: Option<Vec<String>>,
    #[arg(short, long, num_args = 0.., value_delimiter = ' ', help = "Calculates the product of each column.")]
    product: Option<Vec<String>>,
    #[arg(short, long, num_args = 0.., value_delimiter = ' ', help = "Calculates the sum of each column.")]
    sum: Option<Vec<String>>,
    #[arg(long,num_args = 0..,value_delimiter = ' ',help = "Calculates the correlation coefficient of non-null pairs (y,x). Example: '--corr c1,c2 c3,c4 c5,c6'")]
    corr: Option<Vec<String>>,
    #[arg(short = 'E', long, num_args = 0.., value_delimiter = ' ', help = "Returns the log-2 entropy of count input-values for each column.")]
    entropy: Option<Vec<String>>,
    #[arg(short = 'K', long, num_args = 0.., value_delimiter = ' ', help = "Returns the excess kurtosis (Fisher’s definition) of all input values, with a bias correction according to the sample size.")]
    kurtosis: Option<Vec<String>>,
    #[arg(long, num_args = 0.., value_delimiter = ' ', help = "Returns the median absolute deviation for the values within x. NULL values are ignored. Temporal types return a positive INTERVAL.")]
    mad: Option<Vec<String>>,
    #[arg(long, num_args = 0.., value_delimiter = ' ', help = "Returns the middle value of the set. NULL values are ignored. For even value counts, quantitative values are averaged and ordinal values return the lower value.")]
    median: Option<Vec<String>>,
    #[arg(long, num_args = 0.., value_delimiter = ' ', help = "Returns the most frequent value for the values within x. NULL values are ignored.")]
    mode: Option<Vec<String>>,

    #[arg(
        short,
        long,
        default_value_t = 10,
        help = "Limit rows to display. Use -1 to return all rows."
    )]
    limit: i128,
    #[arg(short, long)]
    order_by: Option<Vec<String>>,
    #[arg(short, long)]
    write: Option<Vec<String>>,
}

impl AggCommand {
    pub fn run(&self, conn: Connection, from: String) -> Result<()> {
        let mut query_parts: Vec<String> = vec![String::from("SELECT")];

        let mut column_parts: Vec<String> = vec![];
        if let Some(columns) = &self.count_distinct {
            column_parts.push(join_cols(
                &columns
                    .iter()
                    .map(|c| format!("distinct {c}"))
                    .collect::<Vec<String>>(),
                Some("COUNT"),
            ));
        }

        let functions_and_columns: Vec<(&Option<Vec<String>>, Option<&str>)> = vec![
            (&self.group_by, None),
            (&self.count, Some("COUNT")),
            (&self.avg, Some("AVG")),
            (&self.geomean, Some("GEOMEAN")),
            (&self.list, Some("LIST")),
            (&self.histogram, Some("HISTOGRAM")),
            (&self.max, Some("MAX")),
            (&self.min, Some("MIN")),
            (&self.product, Some("PRODUCT")),
            (&self.sum, Some("SUM")),
            (&self.corr, Some("CORR")),
            (&self.entropy, Some("ENTROPY")),
            (&self.kurtosis, Some("KURTOSIS")),
            (&self.mad, Some("MAD")),
            (&self.median, Some("MEDIAN")),
            (&self.mode, Some("MODE")),
        ];

        for (columns, function) in functions_and_columns {
            if let Some(columns) = columns {
                column_parts.push(join_cols(columns, function));
            }
        }

        query_parts.push(column_parts.join(",\n"));

        query_parts.push(format!("FROM {}", from));

        if let Some(group_by) = &self.group_by {
            query_parts.push(format!("GROUP BY {}", group_by.join(", ")));
        }
        if self.limit.is_positive() {
            query_parts.push(format!("LIMIT {}", self.limit));
        };

        let query = query_parts.join("\n");

        send_display_query(conn, query)?;

        Ok(())
    }
}
