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

pub fn table(rows: &[Vec<String>], widths: &[usize]) {
    let _guard = stdout_lock().lock().unwrap();
    let mut stdout = io::stdout().lock();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            if index > 0 {
                let _ = write!(stdout, "  ");
            }
            let _ = write!(
                stdout,
                "{value:<width$}",
                width = widths.get(index).copied().unwrap_or(value.len())
            );
        }
        let _ = write!(stdout, "\r\n");
    }
    let _ = stdout.flush();
}
