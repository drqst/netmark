use rusqlite::{Connection, params};
use std::sync::Mutex;

pub struct ExternalSqlMetrics {
    connection: Mutex<Option<Connection>>,
}

impl ExternalSqlMetrics {
    pub fn connect(connection_string: &str) -> Result<Self, String> {
        let path = connection_string
            .strip_prefix("sqlite://")
            .unwrap_or(connection_string);
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS netmark_metrics (timestamp_utc TEXT NOT NULL, run_id INTEGER NOT NULL, sent_tcp_bytes INTEGER NOT NULL, sent_udp_bytes INTEGER NOT NULL, received_tcp_bytes INTEGER NOT NULL, received_udp_bytes INTEGER NOT NULL, lost_udp_packets INTEGER NOT NULL, out_of_order_udp_packets INTEGER NOT NULL, jitter_millis INTEGER NOT NULL)",
        ).map_err(|error| error.to_string())?;
        Ok(Self {
            connection: Mutex::new(Some(connection)),
        })
    }

    pub fn write(
        &self,
        timestamp: &str,
        run_id: u64,
        values: &[u64; 8],
        lost: u64,
        out_of_order: u64,
        jitter: u64,
    ) -> Result<(), String> {
        let connection = self.connection.lock().unwrap();
        let Some(connection) = connection.as_ref() else {
            return Ok(());
        };
        connection
            .execute(
                "INSERT INTO netmark_metrics VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    timestamp,
                    run_id,
                    values[1],
                    values[3],
                    values[5],
                    values[7],
                    lost,
                    out_of_order,
                    jitter
                ],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}
