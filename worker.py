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
        # 원본 파일명에서 확장자를 분리 (예: test.mp4 -> test, .mp4)
        name, ext = os.path.splitext(filename)
        output_filename = f"{name}_{res_name}{ext}"
        local_output = f"/tmp/{output_filename}"
        
        print(f"[{filename}] {res_name} 화질 변환 중...")
        
        # FFmpeg 명령어 (비율 유지하며 세로 해상도 변경, h264 코덱 사용)
        cmd = f"ffmpeg -i {local_input} -vf scale=-2:{height} -c:v libx264 -crf 23 -preset fast -c:a aac -y {local_output}"
        
        try:
            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # 4. 변환된 파일을 새로운 'transcoded' 버킷에 업로드 (폴더 구조처럼 저장)
            s3_client.upload_file(local_output, TRANSCODED_BUCKET, f"{res_name}/{output_filename}")
            print(f"[{filename}] {res_name} 업로드 완료!")
            
        except subprocess.CalledProcessError as e:
            print(f"[{filename}] {res_name} 변환 실패: {e}")
        finally:
            # 임시 출력 파일 삭제
            if os.path.exists(local_output):
                os.remove(local_output)

    # 원본 임시 파일 삭제
    if os.path.exists(local_input):
        os.remove(local_input)
        
    print(f"[{filename}] 모든 트랜스코딩 작업 완료!")
    return True