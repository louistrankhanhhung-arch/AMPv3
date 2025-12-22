import logging
import os
import sys
import itertools
from typing import Any

_SEQ = itertools.count(1)


class _SeqFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Inject a monotonically increasing sequence number for deterministic ordering.
        record.seq = next(_SEQ)  # type: ignore[attr-defined]
        return True

def setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    logger = logging.getLogger("amp_smc")
    logger.setLevel(level)
    logger.propagate = False  # avoid double handlers / root propagation

    # Reset handlers to prevent duplicates on hot-reload/redeploy scenarios.
    for h in list(logger.handlers):
        logger.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.addFilter(_SeqFilter())
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)s | %(name)s | %(seq)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
