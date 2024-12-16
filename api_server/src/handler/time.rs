use chrono::DateTime;

pub fn format_timestamp(timestamp: u64) -> String {
    let dt = DateTime::from_timestamp_millis(timestamp as i64).unwrap();
    dt.to_rfc3339()
}
