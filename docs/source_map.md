# Source map

**Business question:** where does pickup delay accumulate in the NYC ride lifecycle, where is it worst,
and do riders escalate problems more where pickups are worse?

We started from the question, listed the facts it needs, and found the authoritative source for each fact.
We did not start from a dataset.

## 1. Source overview

| Source | Grain | Purpose | Retrieval | Role | Main gap |
|---|---|---|---|---|---|
| **HVFHV trip records** (NYC TLC) | one completed trip | lifecycle timestamps, pickup zone, company | **Parquet file** (HTTP, streamed, Content-Length + sha256) | **Authoritative operational source** | no trip ID; on-scene not always a real event; no scheduled-ride flag; completed trips only |
| **Taxi Zone Lookup** (NYC TLC) | one zone | zone name, borough for IDs | **CSV file** (HTTP) | **Reference** | IDs 264/265 are placeholders |
| **Taxi Zone shapefile** (NYC TLC) | one zone polygon | place 311 complaints into zones | **ZIP/shapefile** (HTTP), parsed with the standard library | **Reference** (geometry) | none found: 263 polygons match the 263 geographic lookup IDs |
| **Open-Meteo Historical** | one hour at one grid cell | precipitation, temperature, wind | **REST API, JSON** | **Contextual source** | one cell ~5 km from Central Park for all NYC; modelled, not a station reading |
| **NYC 311 Service Requests** (Socrata `erm2-nwe9`), complaint type *For Hire Vehicle Complaint* | one complaint | rider escalations to the city | **REST API, JSON, paginated** (`count(*)`, then `$order/$limit/$offset`) | **Customer-friction source (contextual)** | **no ride ID, so never linked to rides**; covers all FHV types; sparse; 81% are non-passenger street-safety complaints |

## 2. Question → required facts → source

| Business fact needed | Source · field(s) | Role | Known gap (profiled on 2026-07) |
|---|---|---|---|
| When the rider asked | HVFHV · `request_datetime` | authoritative | may be the booked slot for pre-scheduled rides: 82% of pickup-before-request rows have a request stamped on an exact minute (UNKNOWN; sensitivity published) |
| When the driver arrived | HVFHV · `on_scene_datetime` | **conditional** | 100% populated, but independently observed and ordered for only 93.4% (4.8% equal pickup to the second; 1.75% before request) |
| When the rider was picked up | HVFHV · `pickup_datetime` | authoritative | 1.18% earlier than the request (flagged, excluded from KPI population) |
| When the ride ended | HVFHV · `dropoff_datetime` | authoritative | 2 rides with dropoff not after pickup |
| Trip duration | derived from timestamps | derived | source `trip_time` disagrees with the timestamps for 10.9% of Lyft rides, so it is not used |
| Which company | HVFHV · `hvfhs_license_num` | authoritative | HV0003 = Uber, HV0005 = Lyft (TLC data dictionary) |
| Where the pickup happened | HVFHV · `PULocationID` → lookup | authoritative, zone-level | no coordinates; 0.006% placeholder IDs |
| Rider friction signal | 311 · `descriptor`, `descriptor_2`, `x/y_coordinate_state_plane` | contextual | only 248 of 1,373 are passenger or company complaints, and 237 of those 248 reach the KPI 5 numerator (11 cannot be placed in a zone → the rate is understated by ~4.4%, quantified rather than guessed); 29 records have no coordinates |
| Where the complaint happened | 311 state-plane point → shapefile | derived (point-in-polygon) | 2 fall outside every zone (bridge/river); borough agreement 99.93% |
| Weather at request time | Open-Meteo hourly | contextual | one point represents all NYC |
| Requests that went unserved | — | **not available** | completed trips only |
| Operational interventions (dispatch rules, incentives, surge) | — | **not observable** | proprietary to the companies, absent from public data |

## 3. Owners and what we did not assume

* **HVFHV, Taxi Zone Lookup, shapefile:** published by NYC TLC. The trip records are submitted by the HVFHS licensees.
  We make no claim about how licensees generate `on_scene_datetime`. We only report what the values show.
* **311:** published on NYC Open Data, with complaints routed to TLC (`agency = TLC` for 100% of records).
  The complaint *location* is the address the complainant gave, not a GPS trace.
* **Open-Meteo:** a third-party model API, used purely as context.

## 4. What "complete retrieval" means for each source

| Source | Proof used |
|---|---|
| HVFHV | stream to `.part`; rename only when bytes == Content-Length; sha256 manifest; Parquet footer readable; row count recorded; every calendar day present; no pickups outside the month |
| Zone CSV / shapefile | bytes == Content-Length; sha256; required columns; unique IDs; polygon IDs == lookup IDs |
| Open-Meteo | raw JSON stored before parsing; expected vs received hours (768), duplicates, nulls, timezone echoed |
| NYC 311 | server `count(*)` stored; pages fetched in stable `unique_key` order and stored before parsing; received == count; no duplicate keys across pages; one snapshot per run (the dataset is live), and `--refresh-311` takes a new append-only snapshot |

On rerun, a preserved raw file is reused only if its sha256 still matches its manifest. Otherwise it is fetched
again, and the mismatch is logged.

## 5. Why there is no SQL source

The rubric asks for at least two retrieval modes across SQL, API and files. This project uses **files** (Parquet,
CSV, shapefile ZIP) and **APIs** (Open-Meteo, Socrata 311 with pagination). Every source is what the real
publisher actually offers. None of them is a relational database.

A SQLite copy made only to "query it with SQL" would add a step without adding evidence. So we did not
manufacture one. (Socrata's SoQL `$where/$select/$order` query language is used for the 311 filter and count,
but we count it as API retrieval, not SQL.)
