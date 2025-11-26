#!/bin/bash
set -euo pipefail
# Run SimpleComponentVerifier
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
./gradlew run -PmainClass=decomposition.eval.SimpleComponentVerifier --args='graph_v500_e600_l5_s1.edge' --quiet
