# ebrains-dataproxy-sync

Sync local directory to dataproxy

## Quick start

```python
from ebrains_dataproxy_sync.sync.dataproxy import sync
from pathlib import Path

path_to_local_dir = Path("path/to-my/local/dir")
remote_prefix = "foo-bar"

sync("my-bucket-name", path_to_local_dir, remote_prefix)

# Files will be sync'ed to my-bucket-name/foo-bar/path/to-my/local/dir
```

## License

MIT
