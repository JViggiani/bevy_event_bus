//! Shared utilities for recording performance benchmark results.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// CSV header used for persisted performance benchmark results.
pub const PERFORMANCE_CSV_HEADER: &str = "timestamp_ms,git_hash,run_name,backend,messages_sent,messages_received,payload_size_bytes,send_duration_ms,receive_duration_ms,send_rate_per_sec,receive_rate_per_sec,send_throughput_mb_per_sec,receive_throughput_mb_per_sec,test_name,send_rate_delta_per_sec\n";

#[derive(Debug, Clone)]
struct PerformanceCsvRecord {
    backend: String,
    run_name: String,
    test_name: String,
    payload_size_bytes: usize,
    send_rate_per_sec: f64,
    receive_rate_per_sec: f64,
}

impl PerformanceCsvRecord {
    fn parse(line: &str) -> Option<Self> {
        let columns: Vec<&str> = line.split(',').collect();
        if columns.len() < 14 {
            return None;
        }

        let backend = columns.get(3)?.trim().to_string();
        let run_name = columns.get(2)?.trim().to_string();
        let payload_size_bytes = columns.get(6)?.trim().parse().ok()?;
        let send_rate_per_sec = columns.get(9)?.trim().parse().ok()?;
        let receive_rate_per_sec = columns.get(10)?.trim().parse().ok()?;
        let test_name = columns.get(13)?.trim().to_string();

        Some(Self {
            backend,
            run_name,
            test_name,
            payload_size_bytes,
            send_rate_per_sec,
            receive_rate_per_sec,
        })
    }
}

/// Records a set of performance metrics into the shared CSV log.
///
/// This function preserves the historical CSV schema, adding a new entry that
/// includes the backend identifier together with message throughput metrics. If
/// the CSV file does not yet exist, it will be created with the correct header.
pub fn record_performance_results(
    backend: &str,
    test_name: &str,
    messages_sent: u64,
    messages_received: u64,
    payload_size: usize,
    send_duration: Duration,
    receive_duration: Duration,
    send_rate: f64,
    receive_rate: f64,
    send_throughput_mb: f64,
    receive_throughput_mb: f64,
) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);

    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|hash| hash.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let run_name = std::env::var("BENCH_NAME").unwrap_or_else(|_| test_name.to_string());
    let csv_path = std::env::var("BENCH_CSV_PATH")
        .unwrap_or_else(|_| "event_bus_perf_results.csv".to_string());
    let csv_path_ref = Path::new(&csv_path);

    if let Err(error) = ensure_performance_csv_schema(csv_path_ref) {
        eprintln!("Failed to normalise performance CSV schema: {error}");
    }

    let previous_record = find_previous_record(csv_path_ref, backend, test_name).unwrap_or(None);
    let send_rate_delta = previous_record
        .as_ref()
        .map(|previous| send_rate - previous.send_rate_per_sec)
        .unwrap_or(0.0);
    let receive_rate_delta = previous_record
        .as_ref()
        .map(|previous| receive_rate - previous.receive_rate_per_sec)
        .unwrap_or(0.0);

    let record = format!(
        "{timestamp},{hash},{run},{backend},{sent},{received},{payload},{send_ms},{receive_ms},{send_rate},{receive_rate},{send_throughput},{receive_throughput},{test},{delta}\n",
        timestamp = timestamp_ms,
        hash = git_hash,
        run = run_name,
        backend = backend,
        sent = messages_sent,
        received = messages_received,
        payload = payload_size,
        send_ms = (send_duration.as_secs_f64() * 1000.0) as u64,
        receive_ms = (receive_duration.as_secs_f64() * 1000.0) as u64,
        send_rate = send_rate,
        receive_rate = receive_rate,
        send_throughput = send_throughput_mb,
        receive_throughput = receive_throughput_mb,
        test = test_name,
        delta = send_rate_delta,
    );

    let mut needs_header = true;
    if let Ok(metadata) = fs::metadata(csv_path_ref) {
        if metadata.len() > 0 {
            needs_header = false;
        }
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_path_ref)
        .expect("Failed to open performance results CSV file");

    if needs_header {
        file.write_all(PERFORMANCE_CSV_HEADER.as_bytes())
            .expect("Failed to write performance CSV header");
    }

    file.write_all(record.as_bytes())
        .expect("Failed to write performance CSV record");

    println!("📊 Performance results recorded to: {}", csv_path);

    if let Some(previous) = previous_record {
        println!(
            "Previous {backend} run \"{run}\": send rate {:.0} msg/s, receive rate {:.0} msg/s (payload {} bytes)",
            previous.send_rate_per_sec,
            previous.receive_rate_per_sec,
            previous.payload_size_bytes,
            backend = previous.backend,
            run = previous.run_name,
        );
        println!(
            "Δ send rate: {:+.0} msg/s, Δ receive rate: {:+.0} msg/s",
            send_rate_delta, receive_rate_delta
        );
    } else {
        println!("No previous run recorded for {backend}::{test_name} in {csv_path}.");
    }

    println!(
        "Current {backend} run \"{run_name}\": send rate {:.0} msg/s, receive rate {:.0} msg/s (payload {} bytes)",
        send_rate,
        receive_rate,
        payload_size,
        backend = backend,
        run_name = run_name,
    );
}

fn ensure_performance_csv_schema(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        let mut file = fs::File::create(path)?;
        file.write_all(PERFORMANCE_CSV_HEADER.as_bytes())?;
        return Ok(());
    }

    let mut content = String::new();
    fs::File::open(path)?.read_to_string(&mut content)?;

    if content.is_empty() {
        let mut file = fs::File::create(path)?;
        file.write_all(PERFORMANCE_CSV_HEADER.as_bytes())?;
        return Ok(());
    }

    let mut lines = content.lines();
    let existing_header = lines.next().unwrap_or_default().trim();

    if existing_header == PERFORMANCE_CSV_HEADER.trim() {
        return Ok(());
    }

    let mut rewritten = String::new();
    rewritten.push_str(PERFORMANCE_CSV_HEADER);

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        // Legacy rows lacked a backend column. Assume Kafka for these entries.
        let mut columns: Vec<&str> = line.split(',').collect();
        if columns.len() >= 13 {
            columns.insert(3, "kafka");
            rewritten.push_str(&columns.join(","));
            rewritten.push('\n');
        }
    }

    let mut file = fs::File::create(path)?;
    file.write_all(rewritten.as_bytes())?;
    Ok(())
}

fn find_previous_record(
    path: &Path,
    backend: &str,
    test_name: &str,
) -> std::io::Result<Option<PerformanceCsvRecord>> {
    if !path.exists() {
        return Ok(None);
    }

    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut last: Option<PerformanceCsvRecord> = None;

    for line in reader.lines().skip(1) {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let current = match PerformanceCsvRecord::parse(&line) {
            Some(record) => record,
            None => continue,
        };

        if current.backend != backend || current.test_name != test_name {
            continue;
        }

        last = Some(current);
    }

    Ok(last)
}
