from pathlib import Path
from hashlib import md5
import os
from typing import Callable
import time

skip_files = []

MD5_HASH_FILE = "md5.hash"

def hash_fn(path: Path, filter_fn:Callable[[Path], bool]=None):
    """Return a md5 hash of a file or a directory
    
    Args:
        path: Path to file or directory
        filter_fn: Callable whether specific path should be included in hash calculation.
    Returns:
        md5 hash
    
    Raises:
        Exception: if path is neither file or directory"""
    
    if path.is_file():
        with open(path, "rb") as fp:
            hash = md5(fp.read())
        return hash.hexdigest()
    if path.is_dir():
        hash = md5()

        # the order will influence md5 hash
        # the hash of a directory must be stable
        for f in sorted(os.listdir(path)):
            path_to_file=path / f
            if filter_fn is not None and not filter_fn(path_to_file):
                continue
            hash.update(hash_fn(path_to_file, filter_fn=filter_fn).encode())
        return hash.hexdigest()
    
    raise Exception(f"{path} is neither file nor directory")

def ignore_hash(path: Path):
    return MD5_HASH_FILE in str(path)

def hash_dir(directory: Path):
    """Given a directory, recursively hash all subdirectories and write the result of the hash to 
    md5.hash file. The method will skip md5.hash file.
    
    Args:
        directory: directory to be hashed"""
    for dirpath, dirnames, filenames in os.walk(directory):
        for dirname in dirnames:
            _dir = Path(dirpath) / dirname
            hash_val = hash_fn(_dir, filter_fn=ignore_hash)
            if (_dir / MD5_HASH_FILE).exists():
                os.rename(
                    _dir / MD5_HASH_FILE,
                    f"{str(_dir / MD5_HASH_FILE)}.bk.{round(time.time())}"
                )
            with open(_dir / MD5_HASH_FILE, "w") as fp:
                fp.write(hash_val)
