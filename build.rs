use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is required"));
    let target_debug = out_dir
        .parent()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .expect("unexpected Cargo output layout");
    for name in ["init.sh", "check.sh", "netmark.config"] {
        let source = PathBuf::from(name);
        let destination = target_debug.join(name);
        if let Err(error) = fs::copy(&source, &destination) {
            panic!("cannot copy {name}: {error}");
        }
    }
    println!("cargo:rerun-if-changed=init.sh");
    println!("cargo:rerun-if-changed=check.sh");
    println!("cargo:rerun-if-changed=netmark.config");
}
