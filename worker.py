import os
import time
import subprocess
from celery import Celery
import boto3

celery_app = Celery('video_tasks', broker='redis://redis:6379/0')

celery_app.conf.task_routes = {
    'worker.extract_audio': {'queue': 'audio_queue'},
    'worker.transcode_video': {'queue': 'video_queue'},
}

MINIO_URL = "http://storage:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
ORIGIN_BUCKET = "videos"
TRANSCODED_BUCKET = "transcoded"

s3_client = boto3.client('s3', endpoint_url=MINIO_URL, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def download_origin(filename):
    local_input = f"/tmp/{filename}"
    if not os.path.exists(local_input):
        s3_client.download_file(ORIGIN_BUCKET, filename, local_input)
    return local_input

# 🎧 1. 오디오 추출 워커 (변경 없음)
@celery_app.task
def extract_audio(filename):
    name, _ = os.path.splitext(filename)
    local_input = download_origin(filename)
    local_output = f"/tmp/{name}_audio.m4a"
    s3_key = f"{name}/audio.m4a"

    print(f"[{filename}] 오디오 추출 시작...")
    cmd = f"ffmpeg -i {local_input} -vn -c:a aac -b:a 128k -y {local_output}"
    
    try:
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        s3_client.upload_file(local_output, TRANSCODED_BUCKET, s3_key)
        print(f"[{filename}] 오디오 업로드 완료! ({s3_key})")
    except Exception as e:
        print(f"[{filename}] 오디오 추출 실패: {e}")
    finally:
        if os.path.exists(local_output): os.remove(local_output)

# 🎬 2. 비디오 워커 (병합 로직 추가 ⭐️)
@celery_app.task
def transcode_video(filename, res_name, height):
    name, _ = os.path.splitext(filename)
    local_input = download_origin(filename)
    
    # 임시 파일들
    video_only_output = f"/tmp/{name}_{res_name}_video.mp4"
    downloaded_audio = f"/tmp/{name}_downloaded_audio.m4a"
    final_merged_output = f"/tmp/{name}_{res_name}_final.mp4"
    
    s3_key = f"{name}/{res_name}.mp4"
    audio_s3_key = f"{name}/audio.m4a"

    print(f"[{filename}] {res_name} 비디오(소리 없음) 변환 시작...")
    cmd = f"ffmpeg -i {local_input} -an -vf scale=-2:{height} -c:v libx264 -crf 23 -preset fast -y {video_only_output}"
    
    try:
        # 1. 영상 트랜스코딩 실행
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"[{filename}] {res_name} 비디오 변환 완료. 병합 대기 중...")

        # 2. 오디오 워커가 만들어둔 오디오 파일 다운로드 (최대 30초 대기)
        audio_ready = False
        for _ in range(30):
            try:
                s3_client.download_file(TRANSCODED_BUCKET, audio_s3_key, downloaded_audio)
                audio_ready = True
                break
            except:
                time.sleep(1) # 오디오가 아직 없으면 1초 기다림

        if audio_ready:
            # 3. 비디오와 오디오 병합 (인코딩 없이 합치기만 하므로 1초 컷!)
            print(f"[{filename}] {res_name} 오디오 병합(Muxing) 진행 중...")
            merge_cmd = f"ffmpeg -i {video_only_output} -i {downloaded_audio} -c:v copy -c:a copy -y {final_merged_output}"
            subprocess.run(merge_cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # 4. 최종 파일 S3 업로드
            s3_client.upload_file(final_merged_output, TRANSCODED_BUCKET, s3_key)
            print(f"[{filename}] {res_name} 최종본 업로드 완료! ({s3_key})")
        else:
            print(f"[{filename}] {res_name} 병합 실패: 오디오 파일을 찾을 수 없습니다.")

    except Exception as e:
        print(f"[{filename}] {res_name} 작업 실패: {e}")
    finally:
        # 남은 임시 찌꺼기 파일들 싹 청소
        for temp_file in [video_only_output, downloaded_audio, final_merged_output]:
            if os.path.exists(temp_file):
                os.remove(temp_file)