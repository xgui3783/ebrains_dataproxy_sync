import logging
import os

logger = logging.getLogger(__name__.split(os.path.extsep)[0])
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("[{name}:{levelname}] {message}", style="{")
handler.setFormatter(formatter)

logger.addHandler(handler)
