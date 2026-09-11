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
            client.batch_execute("CREATE TABLE IF NOT EXISTS netmark_metrics (timestamp_utc TEXT NOT NULL, run_id BIGINT NOT NULL, sent_tcp_bytes BIGINT NOT NULL, sent_udp_bytes BIGINT NOT NULL, received_tcp_bytes BIGINT NOT NULL, received_udp_bytes BIGINT NOT NULL, lost_udp_packets BIGINT NOT NULL, out_of_order_udp_packets BIGINT NOT NULL, jitter_millis BIGINT NOT NULL, sent_ip_bytes BIGINT NOT NULL DEFAULT 0, received_ip_bytes BIGINT NOT NULL DEFAULT 0, sent_bytes_per_second BIGINT NOT NULL DEFAULT 0, received_bytes_per_second BIGINT NOT NULL DEFAULT 0)").map_err(|error| error.to_string())?;
            client.batch_execute("CREATE TABLE IF NOT EXISTS netmark_monitor (timestamp_utc TEXT NOT NULL, monitor_id BIGINT NOT NULL, call_id BIGINT NOT NULL, target TEXT NOT NULL, result TEXT NOT NULL, latency_millis BIGINT NOT NULL, detail TEXT NOT NULL)").map_err(|error| error.to_string())?;
            Ok(Self {
                target: connection_string.to_string(),
                backend: Mutex::new(Backend::Postgres(client)),
            })
        } else {
            let path = connection_string
                .strip_prefix("sqlite://")
                .unwrap_or(connection_string);
            let connection = Connection::open(path).map_err(|error| error.to_string())?;
            connection.execute_batch("CREATE TABLE IF NOT EXISTS netmark_metrics (timestamp_utc TEXT NOT NULL, run_id INTEGER NOT NULL, sent_tcp_bytes INTEGER NOT NULL, sent_udp_bytes INTEGER NOT NULL, received_tcp_bytes INTEGER NOT NULL, received_udp_bytes INTEGER NOT NULL, lost_udp_packets INTEGER NOT NULL, out_of_order_udp_packets INTEGER NOT NULL, jitter_millis INTEGER NOT NULL, sent_ip_bytes INTEGER NOT NULL DEFAULT 0, received_ip_bytes INTEGER NOT NULL DEFAULT 0, sent_bytes_per_second INTEGER NOT NULL DEFAULT 0, received_bytes_per_second INTEGER NOT NULL DEFAULT 0)").map_err(|error| error.to_string())?;
            connection.execute_batch("CREATE TABLE IF NOT EXISTS netmark_monitor (timestamp_utc TEXT NOT NULL, monitor_id INTEGER NOT NULL, call_id INTEGER NOT NULL, target TEXT NOT NULL, result TEXT NOT NULL, latency_millis INTEGER NOT NULL, detail TEXT NOT NULL)").map_err(|error| error.to_string())?;
            Ok(Self {
                target: connection_string.to_string(),
                backend: Mutex::new(Backend::Sqlite(connection)),
            })
        }
    }
    pub fn status(&self) -> String {
        format!("connected ({})", self.target)
    }
    pub fn connection_string(&self) -> &str {
        &self.target
    }
    #[allow(clippy::too_many_arguments)]
    pub fn write(
        &self,
        timestamp: &str,
        run_id: u64,
        values: &[u64; 12],
        lost: u64,
        out_of_order: u64,
        jitter: u64,
        sent_bytes_per_second: u64,
        received_bytes_per_second: u64,
    ) -> Result<(), String> {
        match &mut *self.backend.lock().unwrap() {
            Backend::Sqlite(connection) => connection
                .execute(
                    "INSERT INTO netmark_metrics VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        timestamp,
                        run_id,
                        values[1],
                        values[3],
                        values[5],
                        values[7],
                        lost,
                        out_of_order,
                        jitter,
                        values[9],
                        values[11],
                        sent_bytes_per_second,
                        received_bytes_per_second
                    ],
                )
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Backend::Postgres(client) => client
                .execute(
                    "INSERT INTO netmark_metrics VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
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
                        &(values[9] as i64),
                        &(values[11] as i64),
                        &(sent_bytes_per_second as i64),
                        &(received_bytes_per_second as i64),
                    ],
                )
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }
    }

    /// One row per monitor check. All monitor data goes to the external
    /// database; local SQLite only keeps the monitor start/stop status.
    #[allow(clippy::too_many_arguments)]
    pub fn write_monitor(
        &self,
        timestamp: &str,
        monitor_id: u64,
        call_id: u64,
        target: &str,
        result: &str,
        latency_millis: u64,
        detail: &str,
    ) -> Result<(), String> {
        match &mut *self.backend.lock().unwrap() {
            Backend::Sqlite(connection) => connection
                .execute(
                    "INSERT INTO netmark_monitor VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        timestamp,
                        monitor_id,
                        call_id,
                        target,
                        result,
                        latency_millis,
                        detail
                    ],
                )
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Backend::Postgres(client) => client
                .execute(
                    "INSERT INTO netmark_monitor VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &timestamp,
                        &(monitor_id as i64),
                        &(call_id as i64),
                        &target,
                        &result,
                        &(latency_millis as i64),
                        &detail,
                    ],
                )
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }
    }
}

