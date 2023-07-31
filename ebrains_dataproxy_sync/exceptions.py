class SyncLocked(Exception):
    """SyncLocked, raised when directory is locked. Can be overriden by force=True"""