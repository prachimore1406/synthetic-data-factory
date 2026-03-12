#!/usr/bin/env bash
set -ex

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$ROOT_DIR/app"

if [ ! -x "$ROOT_DIR/.venv/Scripts/python.exe" ]; then
  python -m venv "$ROOT_DIR/.venv"
fi

"$ROOT_DIR/.venv/Scripts/python.exe" -m pip install -r "$APP_DIR/requirements.txt"


cd "$ROOT_DIR"
"$ROOT_DIR/.venv/Scripts/python.exe" -m uvicorn app.api.main:app --reload --port 8000 &
"$ROOT_DIR/.venv/Scripts/python.exe" -m streamlit run app/ui/streamlit_app.py &
wait
