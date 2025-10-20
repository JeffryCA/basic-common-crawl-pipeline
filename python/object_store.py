
import io
import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


def get_minio_client() -> Minio:
    endpoint_raw = os.getenv("S3_ENDPOINT", "http://localhost:19000")
    access = os.getenv("S3_ACCESS_KEY", "admin")
    secret = os.getenv("S3_SECRET_KEY", "12345678")
    hostport, secure = endpoint_raw.split("://")[1], endpoint_raw.startswith("https")
    return Minio(hostport, access_key=access, secret_key=secret, secure=secure)


def ensure_bucket_exists() -> None:
    client = get_minio_client()
    bucket_name = os.environ.get("S3_BUCKET")
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)


def upload_blob(name: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    try:
        client = get_minio_client()
        client.put_object(
            bucket_name=os.environ.get("S3_BUCKET"),
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
    load_dotenv()
    client = get_minio_client()
    bucket = os.environ.get("S3_BUCKET")
    found = client.bucket_exists(bucket_name=bucket)
    print(f"Bucket '{bucket}' exists: {found}")

    # Create an empty file and upload it
    test_data = b"Hello, MinIO!"
    success = upload_blob("test-blob.txt", test_data, content_type="text/plain")
    print(f"Upload successful: {success}")
    # Clean up
    if success:
        client.remove_object(bucket_name=bucket, object_name="test-blob.txt")