use postgres::{Client, NoTls};
use rusqlite::{Connection, params};
use std::sync::Mutex;

enum Backend {
    Sqlite(Connection),
    Postgres(Client),
}

pub struct ExternalSqlMetrics {
    target: String,
    backend: Mutex<Backend>,
}

impl ExternalSqlMetrics {
    pub fn connect(connection_string: &str) -> Result<Self, String> {
        if connection_string.starts_with("postgres://")
            || connection_string.starts_with("postgresql://")
        {
            let mut client =
                Client::connect(connection_string, NoTls).map_err(|error| error.to_string())?;
            client.batch_execute("CREATE TABLE IF NOT EXISTS netmark_metrics (timestamp_utc TEXT NOT NULL, run_id BIGINT NOT NULL, sent_tcp_bytes BIGINT NOT NULL, sent_udp_bytes BIGINT NOT NULL, received_tcp_bytes BIGINT NOT NULL, received_udp_bytes BIGINT NOT NULL, lost_udp_packets BIGINT NOT NULL, out_of_order_udp_packets BIGINT NOT NULL, jitter_millis BIGINT NOT NULL)").map_err(|error| error.to_string())?;
            Ok(Self {
                target: connection_string.to_string(),
                backend: Mutex::new(Backend::Postgres(client)),
            })
        } else {
            let path = connection_string
                .strip_prefix("sqlite://")
                .unwrap_or(connection_string);
            let connection = Connection::open(path).map_err(|error| error.to_string())?;
            connection.execute_batch("CREATE TABLE IF NOT EXISTS netmark_metrics (timestamp_utc TEXT NOT NULL, run_id INTEGER NOT NULL, sent_tcp_bytes INTEGER NOT NULL, sent_udp_bytes INTEGER NOT NULL, received_tcp_bytes INTEGER NOT NULL, received_udp_bytes INTEGER NOT NULL, lost_udp_packets INTEGER NOT NULL, out_of_order_udp_packets INTEGER NOT NULL, jitter_millis INTEGER NOT NULL)").map_err(|error| error.to_string())?;
            Ok(Self {
                target: connection_string.to_string(),
                backend: Mutex::new(Backend::Sqlite(connection)),
            })
        }
    }
    pub fn status(&self) -> String {
        format!("connected ({})", self.target)
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
        match &mut *self.backend.lock().unwrap() {
            Backend::Sqlite(connection) => connection
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
                .map_err(|error| error.to_string()),
            Backend::Postgres(client) => client
                .execute(
                    "INSERT INTO netmark_metrics VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    &[
                        &timestamp,
                        &(run_id as i64),
                        &(values[1] as i64),
                        &(values[3] as i64),
                        &(values[5] as i64),
                        &(values[7] as i64),
                        &(lost as i64),
                        &(out_of_order as i64),
                        &(jitter as i64),
                    ],
                )
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_metrics_can_be_read_back() {
        let path = std::env::temp_dir().join(format!(
            "netmark-external-metrics-{}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let sink = ExternalSqlMetrics::connect(&format!("sqlite://{}", path.display())).unwrap();
        sink.write(
            "2026-08-28T00:00:00.000Z",
            7,
            &[1, 1024, 2, 2048, 3, 3072, 4, 4096],
            5,
            6,
            7,
        )
        .unwrap();
        drop(sink);
        let connection = Connection::open(&path).unwrap();
        let row: (u64, u64, u64, u64, u64, u64, u64, u64) = connection.query_row("SELECT run_id, sent_tcp_bytes, sent_udp_bytes, received_tcp_bytes, received_udp_bytes, lost_udp_packets, out_of_order_udp_packets, jitter_millis FROM netmark_metrics", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?))).unwrap();
        assert_eq!(row, (7, 1024, 2048, 3072, 4096, 5, 6, 7));
        let _ = std::fs::remove_file(path);
    }
}
