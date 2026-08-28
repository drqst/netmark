//! Public SDK for driving netmark from other Rust programs.
//!
//! A [`TestRunner`] takes a [`TestProfile`] (built in code or loaded from a YAML
//! test profile), optionally registers Rust callbacks that the profile refers to
//! by name, runs the traffic, and returns a [`RunReport`].
//!
//! ```no_run
//! use netmark::sdk::TestRunner;
//!
//! let report = TestRunner::from_profile_file("profiles/udp-10kbps.yaml".as_ref())?
//!     .hook("warmup", |context| {
//!         println!("starting run {}", context.run_id);
//!         Ok(())
//!     })
//!     .hook("require_no_loss", |context| {
//!         match context.report {
//!             Some(report) if report.lost_udp_packets > 0 => Err("packets were lost".into()),
//!             _ => Ok(()),
//!         }
//!     })
//!     .run()?;
//! assert!(report.passed);
//! # Ok::<(), String>(())
//! ```

use crate::configuration::{self, TestProfile};
use crate::core::{Debrief, Metrics};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// When a hook runs relative to the traffic itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Before,
    After,
}

/// Everything a hook is given about the run it belongs to.
pub struct HookContext<'a> {
    pub phase: Phase,
    pub run_id: u64,
    pub profile: &'a TestProfile,
    pub log_dir: &'a Path,
    pub metrics: &'a Metrics,
    pub elapsed: Duration,
    /// Present for [`Phase::After`] hooks only.
    pub report: Option<&'a RunReport>,
}

pub type Hook = Arc<dyn Fn(&HookContext<'_>) -> Result<(), String> + Send + Sync>;

/// Maps the hook names used in a test profile to Rust callbacks.
#[derive(Clone, Default)]
pub struct HookRegistry {
    hooks: HashMap<String, Hook>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(
        &mut self,
        name: impl Into<String>,
        hook: impl Fn(&HookContext<'_>) -> Result<(), String> + Send + Sync + 'static,
    ) -> &mut Self {
        self.hooks.insert(name.into(), Arc::new(hook));
        self
    }
    pub fn get(&self, name: &str) -> Option<&Hook> {
        self.hooks.get(name)
    }
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.hooks.keys().map(String::as_str)
    }
}

/// The result of a completed run, in the shape callers are expected to assert on.
#[derive(Debug, Clone)]
pub struct RunReport {
    pub run_id: u64,
    pub passed: bool,
    pub result: String,
    pub failure_reason: Option<String>,
    pub elapsed: Duration,
    pub sent_tcp_bytes: u64,
    pub sent_udp_bytes: u64,
    pub received_tcp_bytes: u64,
    pub received_udp_bytes: u64,
    pub lost_udp_packets: u64,
    pub out_of_order_udp_packets: u64,
    pub tcp_jitter_millis: u64,
    pub udp_jitter_millis: u64,
    /// Client/server reconciliation of the run; absent when no client ran or the
    /// server could not be reached for the debrief.
    pub debrief: Option<Debrief>,
}

impl RunReport {
    pub fn sent_bytes(&self) -> u64 {
        self.sent_tcp_bytes + self.sent_udp_bytes
    }
    pub fn received_bytes(&self) -> u64 {
        self.received_tcp_bytes + self.received_udp_bytes
    }
    /// Bytes per second sent over the wall-clock duration of the run.
    pub fn throughput_bytes_per_second(&self) -> u64 {
        let seconds = self.elapsed.as_secs_f64().max(1.0);
        (self.sent_bytes() as f64 / seconds) as u64
    }
}

/// Builder that runs a single netmark test.
pub struct TestRunner {
    profile: TestProfile,
    registry: HookRegistry,
    log_dir: PathBuf,
    verbose: bool,
}

impl TestRunner {
    pub fn new(profile: TestProfile) -> Self {
        Self {
            profile,
            registry: HookRegistry::new(),
            log_dir: crate::default_log_dir(),
            verbose: false,
        }
    }

    pub fn from_profile_file(path: &Path) -> Result<Self, String> {
        configuration::load_test_profile(path)
            .map(Self::new)
            .map_err(|error| format!("cannot load test profile {}: {error}", path.display()))
    }

    pub fn profile(&self) -> &TestProfile {
        &self.profile
    }

    pub fn profile_mut(&mut self) -> &mut TestProfile {
        &mut self.profile
    }

    /// Registers a Rust callback under `name`; a profile's `hooks.before` and
    /// `hooks.after` lists refer to it by that name.
    pub fn hook(
        mut self,
        name: impl Into<String>,
        hook: impl Fn(&HookContext<'_>) -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        self.registry.register(name, hook);
        self
    }

    pub fn hooks(mut self, registry: HookRegistry) -> Self {
        self.registry = registry;
        self
    }

    pub fn log_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.log_dir = dir.into();
        self
    }

    /// Prints the same start and finish lines as `netmark <profile>` auto mode.
    pub fn verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    pub fn run(self) -> Result<RunReport, String> {
        let before = self.resolve(&self.profile.hooks.before)?;
        let after = self.resolve(&self.profile.hooks.after)?;
        let profile = &self.profile;
        let log_dir = self.log_dir.as_path();
        let verbose = self.verbose;

        let crate::ProfileRun {
            run_id,
            metrics,
            elapsed,
            outcome,
            debrief,
        } = crate::execute_profile(profile, log_dir, |run_id, metrics| {
            if verbose {
                println!("auto mode: started run {run_id}");
            }
            for (name, hook) in &before {
                let context = HookContext {
                    phase: Phase::Before,
                    run_id,
                    profile,
                    log_dir,
                    metrics,
                    elapsed: Duration::ZERO,
                    report: None,
                };
                hook(&context).map_err(|error| format!("before hook {name} failed: {error}"))?;
            }
            Ok(())
        })?;

        let totals = metrics.run_totals();
        let (lost, out_of_order) = metrics.udp_status();
        let (tcp_jitter, udp_jitter) = (metrics.tcp_jitter_millis(), metrics.udp_jitter_millis());
        let mut report = RunReport {
            run_id,
            passed: outcome.result == "ok",
            result: outcome.result.to_string(),
            failure_reason: outcome.failure_reason,
            elapsed,
            sent_tcp_bytes: totals[1],
            sent_udp_bytes: totals[3],
            received_tcp_bytes: totals[5],
            received_udp_bytes: totals[7],
            lost_udp_packets: lost,
            out_of_order_udp_packets: out_of_order,
            tcp_jitter_millis: tcp_jitter,
            udp_jitter_millis: udp_jitter,
            debrief,
        };

        let mut hook_failures = Vec::new();
        for (name, hook) in &after {
            let context = HookContext {
                phase: Phase::After,
                run_id,
                profile,
                log_dir,
                metrics: &metrics,
                elapsed,
                report: Some(&report),
            };
            if let Err(error) = hook(&context) {
                hook_failures.push(format!("after hook {name} failed: {error}"));
            }
        }
        if !hook_failures.is_empty() {
            report.passed = false;
            report.result = "fail".to_string();
            let joined = hook_failures.join("; ");
            report.failure_reason = Some(match report.failure_reason.take() {
                Some(existing) => format!("{existing}; {joined}"),
                None => joined,
            });
        }

        if verbose {
            if let Some(debrief) = &report.debrief {
                println!("auto mode: {}", debrief.summary());
            }
            println!(
                "auto mode: run {} finished result={} sent_bytes={}{}",
                report.run_id,
                report.result,
                report.sent_bytes(),
                report
                    .failure_reason
                    .as_deref()
                    .map(|reason| format!(" reason=\"{reason}\""))
                    .unwrap_or_default()
            );
        }
        Ok(report)
    }

    fn resolve(&self, names: &[String]) -> Result<Vec<(String, Hook)>, String> {
        names
            .iter()
            .map(|name| {
                self.registry
                    .get(name)
                    .map(|hook| (name.clone(), Arc::clone(hook)))
                    .ok_or_else(|| {
                        format!("test profile refers to hook \"{name}\", which is not registered")
                    })
            })
            .collect()
    }
}
