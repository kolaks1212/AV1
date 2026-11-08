# پایه: ایمیج رسمی سرور Local API
FROM aiogram/telegram-bot-api:latest

# نصب پایتون، pip، ffmpeg و supervisord
RUN apk add --no-cache \
    python3 \
    py3-pip \
    ffmpeg \
    supervisor \
    tini

WORKDIR /app

# نصب وابستگی‌های پایتون
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# کپی کد ربات و فایل‌های پیکربندی
COPY bot.py .
COPY supervisord.conf /etc/supervisord.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# مسیر پیش‌فرض برای ذخیره‌سازی فایل‌های سرور API (قابل بازنویسی با Volume)
ENV TELEGRAM_WORK_DIR="/var/lib/telegram-bot-api"
ENV TELEGRAM_TEMP_DIR="/tmp/telegram-bot-api"
RUN mkdir -p "$TELEGRAM_WORK_DIR" "$TELEGRAM_TEMP_DIR"

# اعلام پورت‌ها (8080 برای healthcheck ربات، 8081 برای سرور API)
EXPOSE 8080 8081

# اجرای entrypoint به عنوان PID 1
ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh"]
