use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use kerniva_engine::{Maintainer, MaintenanceOptions};

const USAGE: &str = "Usage: kerniva-maintainer --data-dir DIR [--once] [--min-sealed-bytes BYTES] [--max-age SECONDS] [--interval SECONDS] [--retain-covered-segments 0..16]";

struct Args {
    root: PathBuf,
    interval: Duration,
    thresholds: MaintenanceOptions,
    retain_covered_segments: u64,
    once: bool,
}

fn parse_args() -> Result<Option<Args>, String> {
    let mut root = None;
    let mut thresholds = MaintenanceOptions::default();
    let mut interval = Duration::from_secs(1);
    let mut retain_covered_segments = 0;
    let mut once = false;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        let arg = arg.to_str().ok_or("invalid option encoding")?;
        match arg {
            "--help" | "-h" => return Ok(None),
            "--once" => once = true,
            "--data-dir" => root = Some(PathBuf::from(args.next().ok_or("missing --data-dir")?)),
            "--min-sealed-bytes" => thresholds.min_sealed_bytes = number(args.next(), arg)?,
            "--max-age" => thresholds.max_age = Duration::from_secs(number(args.next(), arg)?),
            "--interval" => interval = Duration::from_secs(number(args.next(), arg)?),
            "--retain-covered-segments" => retain_covered_segments = number(args.next(), arg)?,
            _ => return Err(format!("unrecognized option {arg}")),
        }
    }
    if interval.is_zero() {
        return Err("--interval must be greater than zero".into());
    }
    Ok(Some(Args {
        root: root.ok_or("--data-dir is required")?,
        interval,
        thresholds,
        retain_covered_segments,
        once,
    }))
}

fn number(value: Option<OsString>, name: &str) -> Result<u64, String> {
    value
        .and_then(|value| value.to_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| format!("{name} requires a nonnegative integer"))
}

fn run(args: Args) -> Result<(), String> {
    let maintainer =
        Maintainer::open_with_retention(args.root, args.thresholds, args.retain_covered_segments)
            .map_err(|err| err.to_string())?;
    loop {
        let mut failed = false;
        match maintainer.run_pass(args.once) {
            Ok(results) => {
                for (directory, result) in results {
                    match result {
                        Ok(Some(result)) => println!(
                            "db={} cutoff={} snapshot={} reclaimed_segments={} reclaimed_bytes={}",
                            directory.file_name().unwrap_or_default().to_string_lossy(),
                            result.cutoff,
                            result.snapshot,
                            result.reclaimed_segments,
                            result.reclaimed_bytes
                        ),
                        Ok(None) => {}
                        Err(err) => {
                            failed = true;
                            eprintln!("{}: {err}", directory.display());
                        }
                    }
                }
            }
            Err(err) => {
                failed = true;
                eprintln!("{err}");
            }
        }
        if args.once {
            return if failed {
                Err("one or more databases failed maintenance".into())
            } else {
                Ok(())
            };
        }
        std::thread::sleep(args.interval);
    }
}

fn main() {
    let result = match parse_args() {
        Ok(Some(args)) => run(args),
        Ok(None) => {
            println!("{USAGE}");
            Ok(())
        }
        Err(err) => Err(format!("{err}\n{USAGE}")),
    };
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
