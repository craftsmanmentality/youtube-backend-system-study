FROM python:3.9-slim

WORKDIR /app

# FFmpeg 설치 (영상 변환 필수 도구)
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 기본 명령은 FastAPI 실행 (Worker는 docker-compose에서 명령어를 덮어씁니다)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]