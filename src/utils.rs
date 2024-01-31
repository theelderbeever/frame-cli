use duckdb::{
    arrow::{record_batch::RecordBatch, util::pretty::print_batches},
    Connection, Result,
};
use log::debug;

pub fn join_cols(args: &[String], agg: Option<&str>) -> String {
    args.iter()
        .map(|c| match &agg {
            Some(agg) => format!(
                "{}({c}) AS {}_{}",
                agg.to_uppercase(),
                agg.to_lowercase(),
                c.replace([',', ' '], "_")
            ),
            None => c.to_owned(),
        })
        .collect::<Vec<String>>()
        .join(", ")
}

pub fn send_display_query(conn: Connection, query: String) -> Result<()> {
    debug!("{query}");
    let mut stmt = conn.prepare(&query)?;
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs).unwrap();
    Ok(())
}
