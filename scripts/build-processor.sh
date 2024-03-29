#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# CD to script directory
cd "$(dirname "$0")/.."

# Build
echo "Building processor binary ..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/target/processor ./cmd/processor/main.go
echo "Ok."
ls -lh ./build/target
