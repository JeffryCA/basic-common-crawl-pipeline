
import io
from minio import Minio
from minio.error import S3Error


BUCKET_NAME = "commoncrawl"
MINIO_ROOT_USER = "admin"
MINIO_ROOT_PASSWORD = "12345678"

MINIO_CLIENT = Minio(
    "localhost:19000",
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False
)

def upload_blob(name: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    try:
        MINIO_CLIENT.put_object(
            bucket_name=BUCKET_NAME,
            object_name=name,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return True
    except S3Error as e:
        print(f"MinIO upload error {name}: {e.code} - {e}")
        return False
    

if __name__ == "__main__":
    found = MINIO_CLIENT.bucket_exists(bucket_name=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' exists: {found}")