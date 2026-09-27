"""Console + file logging. One log file per month; each line carries the run id."""
from __future__ import annotations

import logging
from pathlib import Path


def build_logger(log_dir: Path, month: str, run_id: str, level: str = "INFO") -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ride_ops_pipeline")
    logger.setLevel(getattr(logging, level, logging.INFO))
    for h in list(logger.handlers):  # a rerun in the same process must not leak file handles
        h.close()
        logger.removeHandler(h)
    logger.propagate = False

    fmt = logging.Formatter(f"%(asctime)s | run={run_id} | %(levelname)s | %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    file_handler = logging.FileHandler(log_dir / f"pipeline_{month}.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger
