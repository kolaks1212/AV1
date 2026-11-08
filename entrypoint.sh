#!/bin/bash
set -e

# پوشه‌های لازم
mkdir -p /var/lib/telegram-bot-api /tmp/telegram-bot-api /data/work

# پورت سرور لوکال
API_PORT="${TELEGRAM_HTTP_PORT:-8081}"

# استارت سرور لوکال در پس‌زمینه
echo "Starting telegram-bot-api on port $API_PORT ..."
telegram-bot-api \
  --api-id="$TELEGRAM_API_ID" \
  --api-hash="$TELEGRAM_API_HASH" \
  --local \
  --http-port="$API_PORT" \
  --dir=/var/lib/telegram-bot-api \
  --temp-dir=/tmp/telegram-bot-api \
  --verbosity=1 &

# صبر کن تا سرور آماده شود
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${API_PORT}/" >/dev/null 2>&1; then
    echo "telegram-bot-api is ready."
    break
  fi
  sleep 1
done

# آدرس داخلی برای ربات
export TELEGRAM_API_BASE="http://127.0.0.1:${API_PORT}"

# استارت ربات
echo "Starting bot..."
exec python bot.py