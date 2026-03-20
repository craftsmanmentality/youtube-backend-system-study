import os
import subprocess
from celery import Celery
import boto3

# Redis를 브로커로 사용하는 Celery 앱 생성
celery_app = Celery('video_tasks', broker='redis://redis:6379/0')

# MinIO 설정
MINIO_URL = "http://storage:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
ORIGIN_BUCKET = "videos"
TRANSCODED_BUCKET = "transcoded"

s3_client = boto3.client('s3', endpoint_url=MINIO_URL, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

@celery_app.task
def transcode_video(filename):
    print(f"[{filename}] 트랜스코딩 작업을 시작합니다...")
    
    # 1. 원본 파일 임시 다운로드
    local_input = f"/tmp/{filename}"
    s3_client.download_file(ORIGIN_BUCKET, filename, local_input)

    # 2. 변환할 화질 목록 (유튜브 방식)
    resolutions = {
        "1080p": 1080,
        "720p": 720,
        "480p": 480,
        "360p": 360
    }

# 3. 화질별로 FFmpeg 실행
    for res_name, height in resolutions.items():
        # 원본 파일명에서 확장자를 분리 (예: "sample.mp4" -> "sample", ".mp4")
        name, ext = os.path.splitext(filename)
        
        # 로컬 임시 파일명 (다른 작업과 안 섞이게 고유하게)
        local_output = f"/tmp/{name}_{res_name}{ext}" 
        
        # ⭐️ S3에 저장될 실제 경로 지정 (비디오이름/해상도.mp4)
        s3_key = f"{name}/{res_name}{ext}"
        
        print(f"[{filename}] {res_name} 화질 변환 중...")
        cmd = f"ffmpeg -i {local_input} -vf scale=-2:{height} -c:v libx264 -crf 23 -preset fast -c:a aac -y {local_output}"
        
        try:
            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # 4. 바뀐 경로(s3_key)로 업로드
            s3_client.upload_file(local_output, TRANSCODED_BUCKET, s3_key)
            print(f"[{filename}] {res_name} 업로드 완료! (경로: {s3_key})")
            
        except subprocess.CalledProcessError as e:
            print(f"[{filename}] {res_name} 변환 실패: {e}")
        finally:
            if os.path.exists(local_output):
                os.remove(local_output)

    # 원본 임시 파일 삭제
    if os.path.exists(local_input):
        os.remove(local_input)
        
    print(f"[{filename}] 모든 트랜스코딩 작업 완료!")
    return True