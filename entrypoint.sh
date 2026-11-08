#!/bin/sh
set -e

# اگر Volume متصل باشد، مسیر آن را از Railway می‌خوانیم
if [ -n "$RAILWAY_VOLUME_MOUNT_PATH" ]; then
    export TELEGRAM_WORK_DIR="$RAILWAY_VOLUME_MOUNT_PATH/telegram-bot-api"
    mkdir -p "$TELEGRAM_WORK_DIR"
    echo "Volume detected. Using TELEGRAM_WORK_DIR=$TELEGRAM_WORK_DIR"
fi

# اجرای supervisord به عنوان فرآیند foreground
exec /usr/bin/supervisord -c /etc/supervisord.conf
