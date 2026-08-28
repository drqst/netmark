//! Using netmark as an SDK: load a test profile, attach Rust code to it by hook
//! name, run it, and assert on the report.
//!
//! Run with: cargo run --example sdk_hooks

use netmark::configuration::TestProfile;
use netmark::sdk::TestRunner;

fn main() -> Result<(), String> {
    let mut profile = TestProfile::default();
    profile.server.enabled = true;
    profile.clients[0].enabled = true;
    profile.traffic.packet_type = "udp".to_string();
    profile.traffic.udp_rate = 10;
    profile.duration_seconds = 3;
    profile.hooks.before = vec!["announce".to_string()];
    profile.hooks.after = vec!["require_traffic".to_string()];

    let report = TestRunner::new(profile)
        .hook("announce", |context| {
            println!("run {} starting in {}", context.run_id, context.log_dir.display());
            Ok(())
        })
        .hook("require_traffic", |context| {
            let report = context.report.expect("after hooks always get a report");
            if report.sent_bytes() == 0 {
                return Err("no traffic was generated".to_string());
            }
            Ok(())
        })
        .run()?;

    println!(
        "run {} {} ({} bytes sent, {} bytes/sec)",
        report.run_id,
        report.result,
        report.sent_bytes(),
        report.throughput_bytes_per_second()
    );
    Ok(())
}
