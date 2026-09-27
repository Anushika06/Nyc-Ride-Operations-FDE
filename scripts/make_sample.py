"""Build the deterministic offline sample in data/sample/ from the REAL preserved raw inputs.

Run once after a full-data run of the same month:
    python run_pipeline.py --month 2026-07
    python scripts/make_sample.py --month 2026-07

What it produces (committed to the repo, ~3 MB, used by `python run_pipeline.py --sample`):
  hvfhv_sample_<month>.parquet   10,000 rides drawn uniformly at random (seed 42) from the raw monthly file,
                                 same schema, original row order preserved
  taxi_zone_lookup.csv            copied unchanged
  taxi_zones.zip                  copied unchanged
  open_meteo_hourly_<month>.json  the preserved weather response, unchanged
  nyc311_fhv_<month>.json         all FHV complaints from the preserved 311 snapshot (pages concatenated)
  SAMPLE_MANIFEST.json            provenance: source checksums, seed, row counts

The sample is labelled as a sample everywhere it is used. It is NOT the complete dataset.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SEED = 42
N_ROWS = 10_000


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(month: str) -> None:
    raw = ROOT / "data" / "raw"
    out = ROOT / "data" / "sample"
    out.mkdir(parents=True, exist_ok=True)

    hv = raw / "hvfhv" / month / f"fhvhv_tripdata_{month}.parquet"
    table = pq.read_table(hv)
    idx = np.sort(np.random.default_rng(SEED).choice(table.num_rows, size=N_ROWS, replace=False))
    pq.write_table(table.take(idx), out / f"hvfhv_sample_{month}.parquet", compression="zstd")

    ref = ROOT / "data" / "reference"
    shutil.copyfile(ref / "taxi_zone_lookup.csv", out / "taxi_zone_lookup.csv")
    shutil.copyfile(ref / "taxi_zones.zip", out / "taxi_zones.zip")

    wman = json.loads((raw / "weather" / month / "_retrieval_manifest.json").read_text())
    shutil.copyfile(raw / "weather" / month / wman["file"], out / f"open_meteo_hourly_{month}.json")

    man311 = json.loads((raw / "nyc311" / month / "_retrieval_manifest.json").read_text())
    snap = raw / "nyc311" / month / man311["snapshot_dir"]
    rows = [r for p in man311["pages"] for r in json.loads((snap / p["file"]).read_bytes())]
    (out / f"nyc311_fhv_{month}.json").write_text(json.dumps(rows), encoding="utf-8")

    manifest = {
        "month": month,
        "label": "SAMPLE - NOT THE COMPLETE DATASET",
        "method": f"{N_ROWS:,} HVFHV rides drawn uniformly without replacement (numpy default_rng seed {SEED}); "
                  "reference, weather and 311 files copied unchanged from the preserved raw inputs",
        "source_rows": table.num_rows, "sample_rows": N_ROWS,
        "source_sha256": {"hvfhv": json.loads((raw / "hvfhv" / month / "_retrieval_manifest.json").read_text())["sha256"],
                          "weather": wman["sha256"], "nyc311_snapshot": man311["snapshot_dir"]},
        "files": {"hvfhv": f"hvfhv_sample_{month}.parquet", "zone_lookup": "taxi_zone_lookup.csv",
                  "zone_shapes": "taxi_zones.zip", "weather": f"open_meteo_hourly_{month}.json",
                  "nyc311": f"nyc311_fhv_{month}.json"},
    }
    manifest["file_sha256"] = {k: sha(out / v) for k, v in manifest["files"].items()}
    (out / "SAMPLE_MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True)
    sys.exit(main(ap.parse_args().month))
