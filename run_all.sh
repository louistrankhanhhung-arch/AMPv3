#!/usr/bin/env bash
set -euo pipefail

# nạp .env khi chạy local
if [ -f .env ]; then export $(grep -v '^#' .env | xargs -d '\n'); fi

python -m pip install -r requirements.txt

# dùng chung DATA_DIR cho cả scanner lẫn bot
export DATA_DIR="${DATA_DIR:-/data}"
mkdir -p "$DATA_DIR"

# chạy scanner (post teaser + cache plan/full) và bot (DM/paywall)
python -u main.py &           # scanner + notifier
python -u bot_telegram.py     # bot Telegram
