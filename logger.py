"""
One shared logger for the whole pipeline. Every module imports `log` from
here instead of setting up its own logging — so every step writes to the
same file and console in the same format.
"""

import logging
import sys

from config import LOG_FILE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(module)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)

log = logging.getLogger("elt")
