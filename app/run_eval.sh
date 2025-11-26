#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
./gradlew run --args='evaluate --example example1 --graph graphs/example1.edge --k 3'
