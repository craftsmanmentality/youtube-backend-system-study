import boto3
from fastapi import FastAPI, UploadFile, File, HTTPException
import os

app = FastAPI()

# MinIO 설정 (docker-compose의 서비스명인 'storage'를 도메인으로 사용)
MINIO_URL = "http://storage:9000" 
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "videos"

# S3 클라이언트 생성 (MinIO는 S3와 호환됩니다)
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

@app.on_event("startup")
def startup_event():
    # 서버가 켜질 때 'videos'라는 버킷이 없으면 자동으로 생성합니다.
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")

@app.post("/upload/")
async def upload_video(file: UploadFile = File(...)):
    try:
        # MinIO 스토리지로 파일 업로드
        s3_client.upload_fileobj(file.file, BUCKET_NAME, file.filename)
        return {
            "message": "성공적으로 업로드 되었습니다!",
            "filename": file.filename,
            "bucket": BUCKET_NAME
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# main.py 하단에 추가
@app.get("/video/{filename}")
async def get_video(filename: str):
    try:
        # 1. 원래대로 S3(MinIO)에서 URL을 생성합니다. (이때는 storage:9000 으로 만들어짐)
        url = s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': BUCKET_NAME, 'Key': filename},
                                            ExpiresIn=3600)
        
        # 2. 브라우저가 접속할 수 있도록 주소를 localhost로 변경해줍니다! ⭐️
        public_url = url.replace("http://storage:9000", "http://localhost:9000")
        
        return {"video_url": public_url}
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")