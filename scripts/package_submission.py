"""Write ../fde-nyc-ride-operations.zip without regenerable large files.

Excluded (the pipeline re-creates them): data/raw/, data/reference/, data/sample_run/,
output/*/ride_journey.parquet for full-data partitions, output/_chaos_*, staging dirs, caches.
Included: code, tests, docs, notebook, data/sample/ (offline sample), and all evidence outputs.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT.parent / f"{ROOT.name}.zip"


def excluded(rel: str) -> bool:
    parts = rel.split("/")
    return (rel.startswith(("data/raw/", "data/reference/", "data/sample_run/"))
            or "/_chaos_" in "/" + rel or "/.staging_" in "/" + rel or rel.endswith((".part", ".pyc"))
            or "__pycache__" in parts or ".git" in parts or ".venv" in parts
            or (rel.endswith("ride_journey.parquet") and not rel.startswith("output/sample/")))


def main() -> None:
    files = sorted(p for p in ROOT.rglob("*") if p.is_file() and not excluded(p.relative_to(ROOT).as_posix()))
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            z.write(p, f"{ROOT.name}/{p.relative_to(ROOT).as_posix()}")
    print(f"{OUT}  {len(files)} files  {OUT.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
