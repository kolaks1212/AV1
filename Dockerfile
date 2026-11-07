FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    PORT=8080

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg \
       ca-certificates \
       tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

RUN mkdir -p /data/work \
    && python -m py_compile bot.py \
    && ffmpeg -hide_banner -encoders 2>/dev/null | grep -q libx264 \
    && ffmpeg -hide_banner -encoders 2>/dev/null | grep -q libx265 \
    && ffmpeg -hide_banner -encoders 2>/dev/null | grep -q libsvtav1

EXPOSE 8080

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "bot.py"]
