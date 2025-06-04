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
from typing import Union

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

def sync_down(bucket_name: str, path_to_sync: Union[Path, str], remote_prefix: Union[Path, str]="."):
    """Sync file or folder from remote bucket
    
    Args:
        bucket_name (str): name of the bucket to sync to
        path_to_sync (str or Path): (local) path where files will be downloaded
        remote_prefix (str or Path): (remote) prefix
        force (bool): overwrite if necessary
    """
    
    path_to_sync = Path(path_to_sync)
    remote_prefix = Path(remote_prefix)
    
    # import when it is needed, so that 
    from ..config import auth_token
    client = BucketApiClient(token=auth_token) if auth_token else BucketApiClient()
    bucket = client.buckets.get_bucket(bucket_name=bucket_name)
    all_files = [f for f in bucket.ls(prefix=str(remote_prefix))]

    def download(f):
        fname = Path(f.name)

        dst_fname = path_to_sync / fname
        dst_fname.parent.mkdir(exist_ok=True, parents=True)
        b = f.get_content()
        with open(path_to_sync / fname, "wb") as fp:
            fp.write(b)
        

    with ThreadPoolExecutor() as exec:
        for p in tqdm(
            exec.map(
                download,
                all_files
            ),
            total=len(all_files)
        ): ...

def sync(bucket_name: str, path_to_sync: Union[Path, str], remote_prefix: Union[Path, str]=".", * , local_relative_to: Union[Path, str]=None, force: bool=False):
    """Sync file or folder to remote bucket.

    Args:
        bucket_name (str): name of the bucket to sync to
        path_to_sync (str or Path): (local) path to the file/directory to upload
        remote_prefix (str or Path): (remote) path to prepend
        local_relative_to (str or Path): (local) path to trim
        force (bool): overwrite if necessary
    """

    path_to_sync = Path(path_to_sync)
    remote_prefix = Path(remote_prefix)
    local_relative_to = local_relative_to or "."

    # import when it is needed, so that 
    from ..config import auth_token
    client = BucketApiClient(token=auth_token)
    bucket = client.buckets.get_bucket(bucket_name=bucket_name)

    if path_to_sync.is_file():
        logger.info("Syncing single file...")
        bucket.upload(str(path_to_sync), str(remote_prefix / path_to_sync.relative_to(local_relative_to)))
        logger.info("Completed!")
        return


    # Depth first
    all_dirs = [_dir for _dir in os.listdir(path_to_sync)
                if (Path(path_to_sync) / _dir).is_dir()]
    for _dir in all_dirs:
        sync(bucket_name, Path(path_to_sync, _dir), remote_prefix, local_relative_to=local_relative_to, force=force)

    md5_file = path_to_sync / MD5_HASH_FILE
    remote_md5_path = remote_prefix / path_to_sync.relative_to(local_relative_to) / MD5_HASH_FILE

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
                logger.info(f"hash does not match. {local_md5_hash!r} != {remote_md5_hash!r}, syncing {str(path_to_sync)!r}!")
            except DoesNotExist:
                logger.info(f"remote {bucket_name}:{remote_md5_path} does not exist, will upload directory.")
    
    all_files = [file
                 for file in os.listdir(path_to_sync)
                 if (Path(path_to_sync) / file).is_file()]
    
    def upload(path_to_file: Path, remote_path: Path):
        retry_counter = 0
        while True:
            retry_counter += 1
            try:
                bucket.upload(path_to_file, str(remote_path))
                break
            except:
                print(f"retrying... {retry_counter}")
                

    with sync_context(bucket, Path(remote_prefix, path_to_sync), force=force) as log:
        with ThreadPoolExecutor() as exec:
            for progress in tqdm(
                exec.map(
                    upload,
                    [Path(path_to_sync, file) for file in all_files],
                    [Path(remote_prefix, path_to_sync.relative_to(local_relative_to), file) for file in all_files],
                ),
                total=len(all_files)
            ): ...
    

__all__ = [
    "sync",
    "sync_down"
]