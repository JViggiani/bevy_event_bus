#!/bin/bash

# Performance testing script for bevy_event_bus
# Usage: ./run_performance_tests.sh [test_name] [bench_name]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
BENCH_NAME=${2:-"performance_run_$(date +%Y%m%d_%H%M%S)"}
CSV_PATH=${BENCH_CSV_PATH:-"event_bus_perf_results.csv"}

echo -e "${GREEN}🚀 Running Event Bus Performance Tests${NC}"
echo -e "${YELLOW}Benchmark Name:${NC} $BENCH_NAME"
echo -e "${YELLOW}CSV Output:${NC} $CSV_PATH"
echo ""

# Export environment variables for the tests
export BENCH_NAME="$BENCH_NAME"
export BENCH_CSV_PATH="$CSV_PATH"

# Function to run a specific test
run_test() {
    local test_name=$1
    echo -e "${GREEN}Running $test_name...${NC}"
    cargo test --test integration_tests "performance_tests::$test_name" --release -- --ignored --nocapture
    echo ""
}

# If specific test provided, run only that test
if [ "$1" != "" ]; then
    run_test "$1"
else
    # Run all performance tests
    echo -e "${YELLOW}Running all performance tests...${NC}"
    echo ""
    
    run_test "test_message_throughput"
    run_test "test_high_volume_small_messages"
    run_test "test_large_message_throughput"
fi

echo -e "${GREEN}✅ Performance testing completed!${NC}"
echo -e "${YELLOW}Results saved to:${NC} $CSV_PATH"

# Show summary of results if CSV exists
if [ -f "$CSV_PATH" ]; then
    echo ""
    echo -e "${GREEN}📊 Latest Results Summary:${NC}"
    echo "----------------------------------------"
    tail -n 5 "$CSV_PATH" | while IFS=',' read -r timestamp git_hash run_name messages_sent messages_received payload_size send_duration receive_duration send_rate receive_rate send_throughput receive_throughput; do
        if [ "$run_name" != "run_name" ]; then  # Skip header
            printf "Test: %-30s | Send Rate: %8.0f msg/s | Receive Rate: %8.0f msg/s | Payload: %5d bytes\n" \
                "$run_name" "$send_rate" "$receive_rate" "$payload_size"
        fi
    done
fi