from ebrains_drive import BucketApiClient
from ebrains_drive.bucket import Bucket
from ebrains_drive.exceptions import DoesNotExist

from pathlib import Path
import os
from contextlib import contextmanager
from io import StringIO
from time import ctime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from ..hash import MD5_HASH_FILE
from ..logger import logger
from ..exceptions import SyncLocked
from ..util import decode_jwt

LOG_FILE = "sync.log"

log_msg_tmpl = "{name}/{sub}: {timestamp}: {op}"

@contextmanager
def sync_context(bucket: Bucket, remote_dir_dst: Path, force=False):
    """Creates a sync context
    
    Args:
        bucket: needed to retrieve some token info
        remote_dir_dst: dest at remote dataproxy to be sync to. Should be a 'directory'"""
    lock_file = remote_dir_dst / ".lock"
    try:
        filehandle = bucket.get_file(str(lock_file))
        if not force:
            content = filehandle.get_content().decode()
            raise SyncLocked(f"{str(remote_dir_dst)} is locked. {content!r} Set force=True to force write.")
    except DoesNotExist:
        pass
    
    # Create .lock file to indicate that a sync operation is in progress
    client: BucketApiClient = bucket.client
    _header, body = decode_jwt(client._token)

    sub = body.get("sub")
    name = body.get("name", "Name unset")

    lockfile_content = StringIO()
    lockfile_content.write(log_msg_tmpl.format(sub=sub, name=name, timestamp=ctime(), op="lock" ))
    lockfile_content.seek(0)
    
    bucket.upload(lockfile_content, str(lock_file))

    # Get and append op log
    log_file = remote_dir_dst / LOG_FILE
    try:
        log_content = bucket.get_file(str(log_file)).get_content().decode()
    except DoesNotExist:
        log_content = ""
    
    def log_msg(msg: str):
        nonlocal log_content
        assert msg is not None
        assert log_content is not None
        log_content = log_msg_tmpl.format(sub=sub, name=name, timestamp=ctime(), op=msg) + "\n" + log_content
        
    log_msg("start sync")

    try:
        yield log_msg
    finally:
        print("leaving context")

        # Remove the .lock file on finish
        filehandle = bucket.get_file(str(lock_file))
        filehandle.delete()

        # Update op log
        log_msg("end sync")

        log_io = StringIO()
        log_io.write(log_content)
        log_io.seek(0)
        bucket.upload(log_io, str(log_file))
        log_io.close()

def sync(bucket_name: str, path_to_sync: Path, prefix: str=".", * , force: bool=False):

    # import when it is needed, so that 
    from ..config import auth_token
    client = BucketApiClient(token=auth_token)
    bucket = client.buckets.get_bucket(bucket_name=bucket_name)

    remote_prefix = Path(prefix)

    md5_file = path_to_sync / MD5_HASH_FILE
    remote_md5_path = remote_prefix / path_to_sync / MD5_HASH_FILE

    # force flag will sync everything, regardless of hash
    if not force:
        if not md5_file.exists():
            logger.info(f"local: {str(path_to_sync)} is not hashed. Will upload without checking hash.")

        else:
            assert md5_file.is_file()
            with open(md5_file, "r") as fp:
                local_md5_hash = fp.read()

            try:
                filehandle = bucket.get_file(str(remote_md5_path))
                remote_md5_hash = filehandle.get_content().decode()
                if local_md5_hash == remote_md5_hash:
                    logger.info(f"hash match! {local_md5_hash!r} == {remote_md5_hash!r}, skipping {str(path_to_sync)!r}!")
                    return
            except DoesNotExist:
                logger.debug(f"remote {bucket_name}:{remote_md5_path} does not exist, will upload directory.")
    
    all_files = [file
                 for file in os.listdir(path_to_sync)
                 if (Path(path_to_sync) / file).is_file()
                 and file != MD5_HASH_FILE]
    
    def upload(path_to_file: Path, remote_path: Path):
        retry_counter = 0
        while True:
            retry_counter += 1
            try:
                bucket.upload(path_to_file, str(remote_path))
                break
            except:
                print(f"retrying... {retry_counter}")
                

    with sync_context(bucket, Path(prefix, path_to_sync), force=force) as log:
        with ThreadPoolExecutor() as exec:
            for progress in tqdm(
                exec.map(
                    upload,
                    [Path(path_to_sync, file) for file in all_files],
                    [Path(prefix, path_to_sync, file) for file in all_files],
                ),
                total=len(all_files)
            ): ...
    
    all_dirs = [_dir for _dir in os.listdir(path_to_sync)
                if (Path(path_to_sync) / _dir).is_dir()]
    for _dir in all_dirs:
        sync(bucket_name, Path(path_to_sync, _dir), prefix, force=force)

__all__ = [
    "sync"
]