#!/usr/bin/env python3
"""Convenience runner for the event bus performance benchmarks.

This script mirrors the previous shell helper while adding richer summaries.
It executes the ignored performance tests in release mode, records the results
through the existing Rust harness, and prints a comparison against the
previous run of the same test based on the CSV log maintained by the tests.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

DEFAULT_TESTS: Sequence[str] = (
    "test_message_throughput",
    "test_high_volume_small_messages",
    "test_large_message_throughput",
)

CSV_DEFAULT_PATH = Path("event_bus_perf_results.csv")


@dataclass
class PerfRecord:
    run_name: str
    test_name: str
    payload_size_bytes: int
    send_rate_per_sec: float
    receive_rate_per_sec: float

    @classmethod
    def from_csv_row(cls, row: Sequence[str]) -> Optional["PerfRecord"]:
        if len(row) < 13:
            return None

        try:
            payload = int(float(row[5]))
            send_rate = float(row[8])
            receive_rate = float(row[9])
        except ValueError:
            return None

        run_name = row[2].strip()
        test_name = row[12].strip()
        return cls(
            run_name=run_name,
            test_name=test_name,
            payload_size_bytes=payload,
            send_rate_per_sec=send_rate,
            receive_rate_per_sec=receive_rate,
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run event bus performance tests.")
    parser.add_argument(
        "test_name",
        nargs="?",
        help="Specific performance test (default: run all)",
    )
    parser.add_argument(
        "bench_name",
        nargs="?",
        help="Label recorded in the CSV (default: timestamped)",
    )
    parser.add_argument(
        "--csv-path",
        default=str(CSV_DEFAULT_PATH),
        help="Path to the results CSV (default: event_bus_perf_results.csv)",
    )

    return parser.parse_args(argv)


def build_bench_name(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return f"performance_run_{datetime.utcnow():%Y%m%d_%H%M%S}"  # UTC for reproducibility


def run_cargo_test(test: str) -> None:
    cmd = [
        "cargo",
        "test",
        "--test",
        "integration_tests",
        f"performance_tests::{test}",
        "--release",
        "--",
        "--ignored",
        "--nocapture",
    ]

    print(f"\033[0;32mRunning {test}...\033[0m")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    print()


def run_selected_tests(tests: Iterable[str]) -> List[str]:
    executed: List[str] = []
    for test in tests:
        run_cargo_test(test)
        executed.append(test)
    return executed


def read_records(csv_path: Path) -> List[PerfRecord]:
    if not csv_path.exists():
        return []

    records: List[PerfRecord] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for index, row in enumerate(reader):
            if index == 0:
                continue  # skip header
            if not row:
                continue
            record = PerfRecord.from_csv_row(row)
            if record:
                records.append(record)
    return records


def summarise_runs(records: List[PerfRecord], tests: Sequence[str]) -> None:
    if not records:
        print("No performance history available yet.")
        return

    history: dict[str, List[PerfRecord]] = {}
    for record in records:
        history.setdefault(record.test_name, []).append(record)

    print("\n\033[0;32m📊 Latest Results Summary:\033[0m")
    print("----------------------------------------")

    for test in tests:
        entries = history.get(test)
        if not entries:
            print(f"{test}: no entries recorded in {len(records)} rows yet.")
            continue

        previous = entries[-2] if len(entries) >= 2 else None
        current = entries[-1]

        if previous:
            print(
                f"Test: {test}\n"
                f"  Previous run '{previous.run_name}': send {previous.send_rate_per_sec:,.0f} msg/s, "
                f"receive {previous.receive_rate_per_sec:,.0f} msg/s (payload {previous.payload_size_bytes} bytes)"
            )

        send_delta = (
            current.send_rate_per_sec - previous.send_rate_per_sec if previous else None
        )
        receive_delta = (
            current.receive_rate_per_sec - previous.receive_rate_per_sec if previous else None
        )

        delta_line = "Δ send n/a, Δ receive n/a"
        if previous:
            delta_line = (
                f"Δ send {send_delta:+,.0f} msg/s, Δ receive {receive_delta:+,.0f} msg/s"
            )

        print(
            f"  Current run '{current.run_name}': send {current.send_rate_per_sec:,.0f} msg/s, "
            f"receive {current.receive_rate_per_sec:,.0f} msg/s (payload {current.payload_size_bytes} bytes)\n"
            f"  {delta_line}\n"
        )


def main(argv: Sequence[str]) -> None:
    args = parse_args(argv)
    csv_path = Path(args.csv_path)
    bench_name = build_bench_name(args.bench_name)

    prev_bench = os.environ.get("BENCH_NAME")
    prev_csv = os.environ.get("BENCH_CSV_PATH")
    os.environ["BENCH_NAME"] = bench_name
    os.environ["BENCH_CSV_PATH"] = str(csv_path)

    tests_to_run: Sequence[str]
    if args.test_name:
        tests_to_run = (args.test_name,)
    else:
        tests_to_run = DEFAULT_TESTS

    print("\033[0;32m🚀 Running Event Bus Performance Tests\033[0m")
    print(f"Benchmark Name: {bench_name}")
    print(f"CSV Output: {csv_path}")
    print()

    try:
        run_selected_tests(tests_to_run)
    except SystemExit as exc:
        raise SystemExit(exc.code)
    finally:
        if prev_bench is None:
            os.environ.pop("BENCH_NAME", None)
        else:
            os.environ["BENCH_NAME"] = prev_bench

        if prev_csv is None:
            os.environ.pop("BENCH_CSV_PATH", None)
        else:
            os.environ["BENCH_CSV_PATH"] = prev_csv

    print("\033[1;33m✅ Performance testing completed!\033[0m")
    print(f"Results saved to: {csv_path}")

    records = read_records(csv_path)
    summarise_runs(records, tests_to_run)


if __name__ == "__main__":
    main(sys.argv[1:])
