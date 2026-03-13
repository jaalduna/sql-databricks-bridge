#!/usr/bin/env bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

cleanup() {
    echo "Stopping services..."
    kill $BACK_PID $FRONT_PID 2>/dev/null
    wait $BACK_PID $FRONT_PID 2>/dev/null
    echo "Done."
}
trap cleanup EXIT INT TERM

# Backend
echo "Starting backend (port 8000)..."
cd "$PROJECT_DIR"
poetry run bridge serve &
BACK_PID=$!

# Frontend
echo "Starting frontend (port 5173)..."
cd "$PROJECT_DIR/frontend"
npm run dev &
FRONT_PID=$!

echo "Backend PID: $BACK_PID | Frontend PID: $FRONT_PID"
echo "Press Ctrl+C to stop both."
wait
