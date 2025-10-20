

import io
import time
import uuid
import tarfile

from object_store import upload_blob


class ShardWriter:

    def __init__(
            self, 
            upload_prefix: str, 
            max_samples_per_shard: int = 1000,
            max_shard_size: int = 100 * 1024 * 1024, 
        ):
        self.tar_buf = io.BytesIO()
        self.tar = tarfile.open(mode="w", fileobj=self.tar_buf)
        self.upload_prefix = upload_prefix.rstrip("/")
        self.max_shard_size = max_shard_size
        self.max_samples_per_shard = max_samples_per_shard
        self.current_sample_count = 0

    def _add_file_to_tar(self, name: str, payload: bytes):
        info = tarfile.TarInfo(name=name)
        info.size = len(payload)
        info.mtime = int(time.time())
        self.tar.addfile(info, io.BytesIO(payload))

    def add_sample(self, tokens_payload: bytes, meta_payload: bytes):
        sample_id = uuid.uuid4().hex
        self._add_file_to_tar(f"{sample_id}.tokens.npy", tokens_payload)
        self._add_file_to_tar(f"{sample_id}.meta.json", meta_payload)
        self.current_sample_count += 1

        if (self.tar_buf.tell() >= self.max_shard_size or
                self.current_sample_count >= self.max_samples_per_shard):
            self.rotate()

    def rotate(self):
        self.tar.close()
        self.tar_buf.seek(0)
        upload_key = f"{self.upload_prefix}/data_{uuid.uuid4().hex}.tar"
        success = upload_blob(upload_key, self.tar_buf.read())
        print(f"Uploaded {upload_key}" if success else f"Failed to upload {upload_key}") 
        # Reset tar
        self.current_sample_count = 0
        self.tar_buf = io.BytesIO()
        self.tar = tarfile.open(mode="w", fileobj=self.tar_buf)


    def flush(self):
        if self.current_sample_count > 0:
            self.rotate()  # uploads and re-inits
        else:
            try:
                self.tar.close()
            except Exception:
                pass