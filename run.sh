#!/usr/bin/env bash
# Run switch wrapper — creates/uses the project venv, then starts the pipeline.
# Usage:  ./run.sh                    full run
#         ./run.sh --states TX CA     limited geography
#         ./run.sh --dry-run          plan and budget only
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

# Install/refresh dependencies only when requirements.txt is newer than the stamp.
STAMP=.venv/.requirements-stamp
if [ ! -f "$STAMP" ] || [ requirements.txt -nt "$STAMP" ]; then
  echo "Installing dependencies..."
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
  touch "$STAMP"
fi

if [ ! -f .env ]; then
  echo "WARNING: no .env found. Copy .env.example to .env and add TAVILY_API_KEY."
  echo "         The run will continue using OpenStreetMap discovery only."
  echo
fi

exec .venv/bin/python run.py "$@"
