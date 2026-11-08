FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    PORT=8080 \
    TELEGRAM_HTTP_PORT=8081

# وابستگی‌های سیستم
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg \
       ca-certificates \
       tini \
       curl \
       liblzma5 \
       libzstd1 \
       libbrotli1 \
    && rm -rf /var/lib/apt/lists/*

# دانلود باینری آماده telegram-bot-api
# (نسخه لینوکس amd64)
RUN curl -L -o /usr/local/bin/telegram-bot-api \
      "https://github.com/tdlib/telegram-bot-api/releases/download/v7.9/telegram-bot-api-linux-x64" \
    && chmod +x /usr/local/bin/telegram-bot-api \
    || (echo "اگر لینک بالا کار نکرد، از روش دیگر استفاده کن" && exit 1)

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh \
    && mkdir -p /data/work /var/lib/telegram-bot-api \
    && python -m py_compile bot.py

EXPOSE 8080

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["./entrypoint.sh"]
