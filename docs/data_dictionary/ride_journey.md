# `ride_journey` — data dictionary

**Grain:** one row = one HVFHV ride in the source file. Rows are never dropped. Rides that fail a rule stay in
the table and carry a reason. File: `output/<month>/ride_journey.parquet` (zstd). The first 1,000 rows are also
in `ride_journey_sample.csv`.

| Column | Type | Source | Meaning |
|---|---|---|---|
| `ride_id` | int64 | derived | Row ordinal in the checksummed raw file (the source has no trip ID; natural-key uniqueness is verified each run) |
| `hvfhs_license_num` | string | HVFHV | HV0003 = Uber, HV0005 = Lyft |
| `request_datetime` | timestamp (NYC local) | HVFHV | As recorded |
| `on_scene_datetime` | timestamp | HVFHV | As recorded. See `on_scene_status` before using it |
| `pickup_datetime` | timestamp | HVFHV | As recorded |
| `dropoff_datetime` | timestamp | HVFHV | As recorded |
| `pickup_zone_id`, `dropoff_zone_id` | int32 | HVFHV | TLC LocationID |
| `pickup_zone`, `pickup_borough`, `dropoff_zone`, `dropoff_borough` | string | Taxi Zone Lookup | Null if the ID is not in the lookup |
| `trip_miles` | float | HVFHV | As recorded (plausibility flagged in validation, not edited) |
| `trip_duration_min` | float | derived | dropoff − pickup. Source `trip_time` is not used (inconsistent for HV0005) |
| `request_to_pickup_min` | float | derived | pickup − request. **Headline KPI input.** Negative values are kept and flagged |
| `request_to_on_scene_min` | float | derived | on_scene − request (driver-arrival stage) |
| `on_scene_to_pickup_min` | float | derived | pickup − on_scene (curb stage) |
| `on_scene_status` | category | derived | `observed` (request ≤ on_scene < pickup), `same_as_pickup`, `before_request`, `after_pickup`, `missing` |
| `validation_status` | category | derived | `valid`, `missing_timestamp`, `pickup_before_request`, `dropoff_not_after_pickup` (first matching reason) |
| `in_kpi_population` | bool | derived | `validation_status == valid`. Population for KPI 1, 2 |
| `in_stage_population` | bool | derived | KPI population AND `on_scene_status == observed`. Population for KPI 3 |
| `in_zone_population` | bool | derived | KPI population AND pickup zone is a real NYC zone (not 264/265/unknown). Population for KPI 4 |
| `request_on_whole_minute` | bool | derived | Request timestamp has :00 seconds. Signals a possible pre-scheduled ride; used only for sensitivity |
| `trip_time_mismatch` | bool | derived | Abs(source trip_time − timestamp duration) > 60 s |
| `request_hour`, `request_weekday` | int8 | derived | From request_datetime; weekday 0 = Monday |
| `is_weekend` | bool | derived | Saturday/Sunday |
| `time_period` | category | derived | overnight 00–05, am_peak 06–09, midday 10–15, pm_peak 16–19, evening 20–23 |
| `weather_hour` | timestamp | derived | floor(request_datetime, hour), the join key to weather |
| `temperature_c`, `precipitation_mm`, `wind_speed_kmh` | float | Open-Meteo | Null when no weather hour matched |
| `precip_band` | category | derived | `dry` (0 mm/h), `light_rain` (< 2.5), `moderate_or_heavy_rain` (≥ 2.5), `weather_unavailable` |

---

# `nyc311_complaints_mapped.csv`: one row per 311 FHV complaint

No ride linkage exists or is attempted. Street addresses are dropped; the mapped zone is kept.

| Column | Meaning |
|---|---|
| `unique_key` | 311 record key (unique, verified) |
| `created_date` | when the complaint was filed (NYC local) |
| `descriptor`, `descriptor_2` | complaint type and reason as filed |
| `category` | `passenger` (Driver Complaint - Passenger, Car Service Company Complaint), `non_passenger` (street safety), `other` |
| `pickup_reliability_reason` | descriptor_2 is Refused Pick-Up, Denied Request or Service Delay/No Show |
| `reported_borough` | borough as recorded by 311 |
| `zone_id`, `zone`, `zone_borough` | taxi zone from point-in-polygon on state-plane x/y (EPSG:2263); null if unmapped |
| `mapping` | `mapped`, `no_coordinates`, `outside_all_zones`, `ambiguous_lowest_id` |

# `zone_breakdown.csv`: one row per pickup zone for the month

| Column | Meaning |
|---|---|
| `pickup_zone_id`, `pickup_zone`, `pickup_borough` | zone |
| `rides_in_kpi_population` | rides feeding KPI 4 |
| `median_request_to_pickup_min`, `p90_request_to_pickup_min` | KPI 4 values |
| `ranked` | meets `min_rides_per_zone` (1,000 in full mode) |
| `hvfhv_pickups_all` | all zone-attributable HVFHV pickups (KPI 5 denominator) |
| `nyc311_complaints_all`, `nyc311_complaints_passenger`, `nyc311_complaints_pickup_reliability` | complaint counts, zero-filled |
| `nyc311_passenger_rate_per_100k` | supporting detail only |
| `nyc311_low_count_flag` | fewer than 5 passenger complaints: do not rank on this rate |
