#!/usr/bin/env bash
set -Eeuo pipefail
PYBIN="$(command -v python3 || command -v python)"
exec "$PYBIN" -m app.run
