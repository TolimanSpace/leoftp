#!/bin/bash

# Function to run cargo fuzz with the given file (without extension)
run_fuzz() {
  filename=$(basename -- "$1")
  filename_without_ext="${filename%.*}"
  cargo +nightly fuzz run "$filename_without_ext" -- -max_len=32768 -timeout=5
}

export -f run_fuzz

# Trap SIGTERM and SIGINT (Ctrl+C) to kill all child processes
trap 'kill 0' SIGTERM SIGINT

# Find all files in ./fuzz_targets, exclude directories, and run them in parallel
find ./fuzz/fuzz_targets -type f | xargs -n 1 -P 0 -I {} bash -c 'run_fuzz "$@"' _ {}