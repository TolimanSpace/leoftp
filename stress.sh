#!/bin/bash

cleanup() {
    echo "Killing cargo run..."
    kill $PID
    exit
}

trap cleanup SIGINT SIGTERM

while true; do
    ./target/release/tester &
    PID=$!

    # Sleep for 2 seconds
    sleep 2

    if kill -0 $PID 2>/dev/null; then
        kill $PID
    fi

    # Copy files from './testdata/rcv/finished' to './testdata/snd/input'
    mv ./testdata/rcv/finished/* ./testdata/snd/input/

    # Loop back to the beginning
done