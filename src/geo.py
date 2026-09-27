"""Deterministic point -> TLC taxi zone mapping, standard library + numpy only.

The TLC taxi zone shapefile and the NYC 311 `x/y_coordinate_state_plane` fields are
both in NAD83 / New York Long Island State Plane (feet, EPSG:2263), so no map
projection is needed: a complaint's state-plane point is tested against each zone
polygon with the even-odd ray-casting rule (holes and multi-part zones included).

Rules (documented, deterministic):
  * a point inside exactly one zone      -> that zone
  * a point inside several zones (overlap) -> lowest LocationID, counted as ambiguous
  * a point inside no zone / no coordinates -> unmapped (kept, counted, never guessed)
"""
from __future__ import annotations

import io
import struct
import zipfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass
class ZoneShape:
    location_id: int
    zone: str
    borough: str
    bbox: tuple          # xmin, ymin, xmax, ymax
    rings: list          # list of (N, 2) float arrays


def _read_dbf(data: bytes) -> list[dict]:
    n_records, header_len, record_len = struct.unpack("<4xIHH", data[:12])
    fields, pos = [], 32
    while data[pos] != 0x0D:
        name = data[pos:pos + 11].split(b"\0")[0].decode("ascii")
        fields.append((name, data[pos + 16]))
        pos += 32
    rows = []
    for i in range(n_records):
        rec = data[header_len + i * record_len: header_len + (i + 1) * record_len]
        off, row = 1, {}  # byte 0 is the deletion flag
        for name, length in fields:
            row[name] = rec[off:off + length].decode("utf-8", "replace").strip()
            off += length
        rows.append(row)
    return rows


def _read_polygons(data: bytes) -> list[tuple]:
    """Parse shape type 5 (Polygon) records -> [(bbox, [ring arrays])]."""
    shapes, pos = [], 100
    while pos < len(data):
        _, content_len = struct.unpack(">ii", data[pos:pos + 8])
        rec = data[pos + 8: pos + 8 + content_len * 2]
        pos += 8 + content_len * 2
        shape_type = struct.unpack("<i", rec[:4])[0]
        if shape_type == 0:
            shapes.append(((0, 0, 0, 0), []))
            continue
        if shape_type != 5:
            raise ValueError(f"unsupported shape type {shape_type}")
        bbox = struct.unpack("<4d", rec[4:36])
        n_parts, n_points = struct.unpack("<ii", rec[36:44])
        parts = list(struct.unpack(f"<{n_parts}i", rec[44:44 + 4 * n_parts]))
        pts = np.frombuffer(rec[44 + 4 * n_parts: 44 + 4 * n_parts + 16 * n_points], dtype="<f8").reshape(-1, 2)
        bounds = parts + [n_points]
        shapes.append((bbox, [pts[bounds[i]:bounds[i + 1]] for i in range(n_parts)]))
    return shapes


def load_zone_shapes(zip_path: Path) -> list[ZoneShape]:
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
        shp = z.read(next(n for n in names if n.endswith(".shp")))
        dbf = z.read(next(n for n in names if n.endswith(".dbf")))
    polys, attrs = _read_polygons(shp), _read_dbf(dbf)
    if len(polys) != len(attrs):
        raise ValueError(f"shapefile mismatch: {len(polys)} shapes vs {len(attrs)} attribute rows")
    return [ZoneShape(int(float(a["LocationID"])), a["zone"], a["borough"], bbox, rings)
            for a, (bbox, rings) in zip(attrs, polys)]


def _inside(x: np.ndarray, y: np.ndarray, ring: np.ndarray) -> np.ndarray:
    """Even-odd ray casting for many points against one ring."""
    xa, ya = ring[:, 0][:, None], ring[:, 1][:, None]            # edge start (vertices x 1)
    xb, yb = np.roll(ring[:, 0], 1)[:, None], np.roll(ring[:, 1], 1)[:, None]
    crosses = (ya > y) != (yb > y)                                 # vertices x points
    with np.errstate(divide="ignore", invalid="ignore"):
        x_at = (xb - xa) * (y - ya) / (yb - ya) + xa
    return (np.count_nonzero(crosses & (x < x_at), axis=0) % 2) == 1


def map_points(x: np.ndarray, y: np.ndarray, shapes: list[ZoneShape]) -> tuple[np.ndarray, np.ndarray]:
    """Return (location_id or -1, ambiguous flag) per point."""
    n = len(x)
    result = np.full(n, -1, dtype=np.int32)
    hits = np.zeros(n, dtype=np.int16)
    valid = ~(np.isnan(x) | np.isnan(y))
    for s in sorted(shapes, key=lambda s: s.location_id):
        xmin, ymin, xmax, ymax = s.bbox
        cand = valid & (x >= xmin) & (x <= xmax) & (y >= ymin) & (y <= ymax)
        if not cand.any():
            continue
        idx = np.where(cand)[0]
        inside = np.zeros(len(idx), dtype=bool)
        for ring in s.rings:
            inside ^= _inside(x[idx], y[idx], ring)
        hit = idx[inside]
        hits[hit] += 1
        first = hit[result[hit] == -1]
        result[first] = s.location_id
    return result, hits > 1
