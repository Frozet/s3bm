import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError
from fastapi import FastAPI, UploadFile, HTTPException
from dotenv import load_dotenv
import os

load_dotenv()

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION_NAME = os.getenv("S3_REGION_NAME")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

# Инициализация клиента S3
s3_client = boto3.client(
    "s3",
    region_name=S3_REGION_NAME,
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=boto3.session.Config(s3={"addressing_style": "virtual"})
)

app = FastAPI()

# Загрузка файла
@app.post("/upload/")
async def upload_file(file: UploadFile):
    try:
        s3_client.upload_fileobj(file.file, S3_BUCKET_NAME, file.filename)
        return {"message": f"File '{file.filename}' uploaded successfully."}
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="Invalid S3 credentials")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Просмотр списка файлов
@app.get("/files/")
async def list_files():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        files = [obj["Key"] for obj in response.get("Contents", [])]
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Скачивание файла
@app.get("/download/{filename}")
async def download_file(filename: str):
    try:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": filename},
            ExpiresIn=3600  # Срок действия ссылки (в секундах)
        )
        return {"download_url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Удаление файла
@app.delete("/delete/{filename}")
async def delete_file(filename: str):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=filename)
        return {"message": f"File '{filename}' deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))