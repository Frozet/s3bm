import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, Query, UploadFile, HTTPException
import aioftp
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.DEBUG)
boto3.set_stream_logger("botocore")

load_dotenv()

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION_NAME = os.getenv("S3_REGION_NAME")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT"))
FTP_USER = os.getenv("FTP_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD")
FTP_BUCKET = os.getenv("FTP_BUCKET")

# Инициализация клиента S3
s3_client = boto3.client(
    "s3",
    region_name=S3_REGION_NAME,
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=S3_ENDPOINT_URL,
)

app = FastAPI()

# Загрузка файла на FTP
async def upload_to_ftp(file: UploadFile, destination_path: str):
    async with aioftp.Client.context(FTP_HOST, FTP_PORT, user=FTP_USER, password=FTP_PASSWORD) as client:
        # Проверяем, существует ли директория, и создаем ее, если необходимо
        try:
            await client.stat(destination_path)
        except aioftp.StatusCodeError:  # Директория не существует
            await client.make_directory(destination_path)
        
        # Загрузка файла
        destination_file = f"{destination_path}/{file.filename}"

        async with client.upload_stream(destination_file) as stream:
            while chunk := await file.read(1024 * 1024):  # Чтение файла по 1 МБ
                await stream.write(chunk)

# Функция для загрузки файла в S3
def upload_to_s3(file: UploadFile, bucket: str, key: str):
    s3_client = boto3.client(
        "s3",
        region_name=S3_REGION_NAME,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    try:
        s3_client.upload_fileobj(file.file, bucket, key)
    except (BotoCoreError, ClientError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")

# Загрузка файла
@app.post("/upload/")
async def upload_file(
    file: UploadFile,
    storage_type: str = Query("ftp", enum=["ftp", "s3"]),  # Выбор между FTP и S3
):
    try:
        if storage_type == "ftp":
            # Путь назначения на FTP-сервере
            destination_path = f"/{FTP_BUCKET}"
            # Загружаем файл на FTP-сервер
            await upload_to_ftp(file, destination_path)
            return {"message": f"File '{file.filename}' successfully uploaded to bucket '{FTP_BUCKET}' on FTP."}
        elif storage_type == "s3":
            # Загружаем файл в S3
            s3_key = f"{S3_BUCKET_NAME}/{file.filename}".strip("/")
            upload_to_s3(file, S3_BUCKET_NAME, s3_key)
            return {"message": f"File '{file.filename}' successfully uploaded to S3 bucket '{S3_BUCKET_NAME}'!"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

# Просмотр файлов
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