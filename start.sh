#!/usr/bin/env bash
set -euo pipefail
PYBIN="python3"; command -v python3 >/dev/null || PYBIN="python"
exec "$PYBIN" "app/run.py"
