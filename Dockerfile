FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HLS_DIR=/tmp/hls \
    PORT=8000

RUN apk add --no-cache \
    ffmpeg \
    bento4 \
    ca-certificates

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn httpx

COPY app.py /app/app.py

RUN mkdir -p /tmp/hls

EXPOSE 8000

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]