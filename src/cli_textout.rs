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

pub fn lines<I, S>(values: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let _guard = stdout_lock().lock().unwrap();
    let mut stdout = io::stdout().lock();
    for value in values {
        let value = value.as_ref().trim_end_matches(['\r', '\n']);
        let _ = write!(stdout, "{value}\r\n");
    }
    let _ = stdout.flush();
}

pub fn raw(text: impl AsRef<str>) {
    let _guard = stdout_lock().lock().unwrap();
    let mut stdout = io::stdout().lock();
    let _ = write!(stdout, "{}", text.as_ref());
    let _ = stdout.flush();
}
