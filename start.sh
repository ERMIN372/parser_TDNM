export WEBAPP_PORT=5000
#!/usr/bin/env bash
set -euo pipefail
PYBIN="python3"; command -v python3 >/dev/null 2>&1 || PYBIN="python"
exec "$PYBIN" -m app.run
