use std::io::{self, Write};
use std::sync::{Mutex, OnceLock};

static STDOUT: OnceLock<Mutex<()>> = OnceLock::new();

fn stdout_lock() -> &'static Mutex<()> {
    STDOUT.get_or_init(|| Mutex::new(()))
}

pub fn line(text: impl AsRef<str>) {
    let _guard = stdout_lock().lock().unwrap();
    let text = text.as_ref().trim_end_matches(['\r', '\n']);
    let mut stdout = io::stdout().lock();
    let _ = write!(stdout, "{text}\r\n");
    let _ = stdout.flush();
}

pub fn raw(text: impl AsRef<str>) {
    let _guard = stdout_lock().lock().unwrap();
    let mut stdout = io::stdout().lock();
    let _ = write!(stdout, "{}", text.as_ref());
    let _ = stdout.flush();
}

/// Formats `rows` into aligned text lines using `widths`, shared by the terminal
/// table printer and every surface that returns table output as text (status,
/// help and the SCTP page in the web CLI).
pub fn table_lines(rows: &[Vec<String>], widths: &[usize]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            let mut line = String::new();
            for (index, value) in row.iter().enumerate() {
                if index > 0 {
                    line.push_str("  ");
                }
                let width = widths.get(index).copied().unwrap_or(value.len());
                line.push_str(&format!("{value:<width$}"));
            }
            line
        })
        .collect()
}

pub fn table(rows: &[Vec<String>], widths: &[usize]) {
    let _guard = stdout_lock().lock().unwrap();
    let mut stdout = io::stdout().lock();
    for line in table_lines(rows, widths) {
        let _ = write!(stdout, "{line}\r\n");
    }
    let _ = stdout.flush();
}
