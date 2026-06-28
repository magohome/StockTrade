#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_BIN="$PYTHON"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "未找到 Python 解释器。请设置 PYTHON=/path/to/python 后重试。" >&2
  exit 127
fi

"$PYTHON_BIN" -m pipeline.fetch_incremental
"$PYTHON_BIN" -m pipeline.cli preselect
"$PYTHON_BIN" -m pipeline.calc_sector_turnover
