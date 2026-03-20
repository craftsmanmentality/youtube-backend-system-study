import boto3
from fastapi import FastAPI, UploadFile, File, HTTPException
import os
from worker import transcode_video # 워커의 태스크 임포트

app = FastAPI()

MINIO_URL = "http://storage:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
ORIGIN_BUCKET = "videos"
TRANSCODED_BUCKET = "transcoded" # 새로운 버킷 추가

s3_client = boto3.client('s3', endpoint_url=MINIO_URL, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

@app.on_event("startup")
def startup_event():
    # 원본 버킷 및 트랜스코딩 버킷 생성
    for bucket in [ORIGIN_BUCKET, TRANSCODED_BUCKET]:
        try:
            s3_client.head_bucket(Bucket=bucket)
        except:
            s3_client.create_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' created.")

@app.post("/upload/")
async def upload_video(file: UploadFile = File(...)):
    try:
        # 1. MinIO 스토리지로 원본 파일 업로드
        s3_client.upload_fileobj(file.file, ORIGIN_BUCKET, file.filename)
        
        # 2. 백그라운드 워커(Celery)에게 화질 변환 작업 지시 (.delay를 붙여야 비동기로 실행됨)
        transcode_video.delay(file.filename)
        
        return {
            "message": "성공적으로 업로드 되었으며, 화질 변환 작업이 백그라운드에서 시작되었습니다!",
            "filename": file.filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 1. 원본 영상 가져오기
@app.get("/video/origin/{filename}")
async def get_origin_video(filename: str):
    try:
        url = s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': ORIGIN_BUCKET, 'Key': filename},
                                            ExpiresIn=3600)
        return {"video_url": url.replace("http://storage:9000", "http://localhost:9000")}
    except Exception as e:
        raise HTTPException(status_code=404, detail="원본 파일을 찾을 수 없습니다.")

# 2. 트랜스코딩된 영상 가져오기 (폴더 경로 포함 ⭐️)
@app.get("/video/transcoded/{resolution}/{filename}")
async def get_transcoded_video(resolution: str, filename: str):
    # 실제 S3에 저장된 Key 구조 (예: "720p/sample_720p.mp4")
    object_key = f"{resolution}/{filename}" 
    
    try:
        url = s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': TRANSCODED_BUCKET, 'Key': object_key},
                                            ExpiresIn=3600)
        return {"video_url": url.replace("http://storage:9000", "http://localhost:9000")}
    except Exception as e:
        raise HTTPException(status_code=404, detail="변환된 파일을 찾을 수 없습니다.")
    

@app.get("/videos/{bucket_type}")
async def list_videos(bucket_type: str):
    # bucket_type은 'origin' 또는 'transcoded'로 받습니다.
    target_bucket = ORIGIN_BUCKET if bucket_type == "origin" else TRANSCODED_BUCKET
    
    try:
        response = s3_client.list_objects_v2(Bucket=target_bucket)
        # 파일이 하나도 없을 경우 처리
        if 'Contents' not in response:
            return {"message": f"'{target_bucket}' 버킷이 비어 있습니다.", "files": []}
            
        files = [obj['Key'] for obj in response.get('Contents', [])]
        return {
            "bucket": target_bucket,
            "count": len(files),
            "file_list": files
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))