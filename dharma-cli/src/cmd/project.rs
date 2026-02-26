use crate::DharmaError;
use std::time::Duration;

const DEFAULT_SCOPE: &str = "std.commerce";
const DEFAULT_INTERVAL: &str = "5s";

pub fn project(args: &[&str]) -> Result<(), DharmaError> {
    match args.split_first() {
        Some((&"rebuild", rest)) => {
            let opts = parse_rebuild_args(rest)?;
            crate::cmd::project_runtime::rebuild(&opts.scope)?;
            Ok(())
        }
        Some((&"watch", rest)) => {
            let opts = parse_watch_args(rest)?;
            crate::cmd::project_runtime::watch(&opts.scope, opts.interval)?;
            Ok(())
        }
        _ => {
            print_usage();
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RebuildOptions {
    scope: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WatchOptions {
    scope: String,
    interval: Duration,
}

fn parse_rebuild_args(args: &[&str]) -> Result<RebuildOptions, DharmaError> {
    let mut scope = DEFAULT_SCOPE.to_string();
    let mut i = 0usize;
    while i < args.len() {
        let arg = args[i];
        if arg == "--scope" {
            i += 1;
            if i >= args.len() {
                return Err(DharmaError::Validation(
                    "missing value after --scope".to_string(),
                ));
            }
            scope = normalize_scope(args[i])?;
        } else if let Some(value) = arg.strip_prefix("--scope=") {
            scope = normalize_scope(value)?;
        } else {
            return Err(DharmaError::Validation(format!(
                "unknown project rebuild argument: {arg}"
            )));
        }
        i += 1;
    }
    Ok(RebuildOptions { scope })
}

fn parse_watch_args(args: &[&str]) -> Result<WatchOptions, DharmaError> {
    let mut scope = DEFAULT_SCOPE.to_string();
    let mut interval = parse_interval(DEFAULT_INTERVAL)?;
    let mut i = 0usize;
    while i < args.len() {
        let arg = args[i];
        if arg == "--scope" {
            i += 1;
            if i >= args.len() {
                return Err(DharmaError::Validation(
                    "missing value after --scope".to_string(),
                ));
            }
            scope = normalize_scope(args[i])?;
        } else if let Some(value) = arg.strip_prefix("--scope=") {
            scope = normalize_scope(value)?;
        } else if arg == "--interval" {
            i += 1;
            if i >= args.len() {
                return Err(DharmaError::Validation(
                    "missing value after --interval".to_string(),
                ));
            }
            interval = parse_interval(args[i])?;
        } else if let Some(value) = arg.strip_prefix("--interval=") {
            interval = parse_interval(value)?;
        } else {
            return Err(DharmaError::Validation(format!(
                "unknown project watch argument: {arg}"
            )));
        }
        i += 1;
    }
    Ok(WatchOptions { scope, interval })
}

fn normalize_scope(scope: &str) -> Result<String, DharmaError> {
    let mut normalized = scope.trim().to_string();
    if normalized.is_empty() {
        return Err(DharmaError::Validation("scope cannot be empty".to_string()));
    }
    while normalized.ends_with('*') {
        normalized.pop();
    }
    while normalized.ends_with('.') {
        normalized.pop();
    }
    if normalized.is_empty() {
        return Err(DharmaError::Validation(
            "scope cannot be wildcard-only".to_string(),
        ));
    }
    Ok(normalized)
}

fn parse_interval(value: &str) -> Result<Duration, DharmaError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation(
            "interval cannot be empty".to_string(),
        ));
    }
    let millis = if let Some(rest) = trimmed.strip_suffix("ms") {
        parse_positive_u64(rest, "interval")?
    } else if let Some(rest) = trimmed.strip_suffix('s') {
        parse_positive_u64(rest, "interval")?
            .checked_mul(1_000)
            .ok_or_else(|| DharmaError::Validation("interval overflow".to_string()))?
    } else if let Some(rest) = trimmed.strip_suffix('m') {
        parse_positive_u64(rest, "interval")?
            .checked_mul(60_000)
            .ok_or_else(|| DharmaError::Validation("interval overflow".to_string()))?
    } else if let Some(rest) = trimmed.strip_suffix('h') {
        parse_positive_u64(rest, "interval")?
            .checked_mul(3_600_000)
            .ok_or_else(|| DharmaError::Validation("interval overflow".to_string()))?
    } else {
        parse_positive_u64(trimmed, "interval")?
            .checked_mul(1_000)
            .ok_or_else(|| DharmaError::Validation("interval overflow".to_string()))?
    };
    Ok(Duration::from_millis(millis))
}

fn parse_positive_u64(raw: &str, label: &str) -> Result<u64, DharmaError> {
    let value = raw
        .trim()
        .parse::<u64>()
        .map_err(|_| DharmaError::Validation(format!("invalid {label}")))?;
    if value == 0 {
        return Err(DharmaError::Validation(format!("{label} must be > 0")));
    }
    Ok(value)
}

fn print_usage() {
    println!("Usage:");
    println!("  dh project rebuild [--scope std.commerce]");
    println!("  dh project watch [--scope std.commerce] [--interval 5s]");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rebuild_defaults_scope() {
        let opts = parse_rebuild_args(&[]).unwrap();
        assert_eq!(opts.scope, "std.commerce");
    }

    #[test]
    fn parse_rebuild_normalizes_scope_wildcard() {
        let opts = parse_rebuild_args(&["--scope", "std.commerce.*"]).unwrap();
        assert_eq!(opts.scope, "std.commerce");
    }

    #[test]
    fn parse_watch_defaults() {
        let opts = parse_watch_args(&[]).unwrap();
        assert_eq!(opts.scope, "std.commerce");
        assert_eq!(opts.interval, Duration::from_secs(5));
    }

    #[test]
    fn parse_watch_accepts_interval_suffixes() {
        assert_eq!(
            parse_watch_args(&["--interval", "750ms"]).unwrap().interval,
            Duration::from_millis(750)
        );
        assert_eq!(
            parse_watch_args(&["--interval=3s"]).unwrap().interval,
            Duration::from_secs(3)
        );
        assert_eq!(
            parse_watch_args(&["--interval=2m"]).unwrap().interval,
            Duration::from_secs(120)
        );
    }

    #[test]
    fn parse_watch_rejects_invalid_interval() {
        assert!(parse_watch_args(&["--interval", "0"]).is_err());
        assert!(parse_watch_args(&["--interval", "abc"]).is_err());
    }
}
