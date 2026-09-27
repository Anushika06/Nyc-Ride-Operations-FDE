"""PUBLISH: idempotent, all-or-nothing replacement of one month's partition.

Everything is written into a staging directory first. Only when every file is
written is the staging directory swapped in for output/<month>/. A crash midway
leaves the previous partition untouched, and a rerun replaces rather than appends.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path


def write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def swap_in_partition(staging: Path, final: Path, logger) -> None:
    backup = final.with_name(final.name + ".previous")
    if backup.exists():
        shutil.rmtree(backup)
    if final.exists():
        final.rename(backup)          # keep the old partition until the new one is in place
    try:
        staging.rename(final)
    except Exception:
        if backup.exists() and not final.exists():
            backup.rename(final)      # roll back
        raise
    if backup.exists():
        shutil.rmtree(backup)
    logger.info("Partition published atomically | %s", final)


def new_staging_dir(output_dir: Path, month: str, run_id: str) -> Path:
    staging = output_dir / f".staging_{month}_{run_id}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    return staging


def clean_stale_staging(output_dir: Path, month: str, logger) -> None:
    """Remove staging dirs left behind by crashed runs of this month."""
    if not output_dir.exists():
        return
    for d in output_dir.glob(f".staging_{month}_*"):
        logger.warning("Removing stale staging directory from an earlier crashed run | %s", d)
        shutil.rmtree(d, ignore_errors=True)
