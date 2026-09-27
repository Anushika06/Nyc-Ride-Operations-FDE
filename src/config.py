"""Pipeline configuration.

Code = behaviour. Configuration = where/how it runs and the documented thresholds.
Every threshold below is a business assumption, not a fact about the data; each one
is echoed into validation_report.json so a reader can see what the run assumed.
Override any value with an environment variable of the same name.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Config:
    month: str  # YYYY-MM, the logical run partition

    # --- locations -------------------------------------------------------
    data_dir: Path
    output_dir: Path
    log_dir: Path

    # --- sources ---------------------------------------------------------
    hvfhv_url_template: str
    zone_lookup_url: str
    zone_shapes_url: str
    weather_api_url: str
    # Reference point for weather. Central Park (NWS station KNYC).
    # Open-Meteo snaps this to its nearest grid cell; the actual cell is recorded.
    weather_latitude: float
    weather_longitude: float
    weather_timezone: str
    weather_variables: tuple
    nyc311_api_url: str
    nyc311_page_size: int

    # --- retrieval reliability ------------------------------------------
    http_timeout_seconds: float
    max_retries: int
    retry_base_seconds: float
    retry_max_wait_seconds: float

    # --- validation thresholds (business assumptions) --------------------
    # Share of rides that may be excluded from the KPI population before the
    # headline metric is considered unrepresentative (FAIL).
    max_excluded_share: float
    # Share of natural-key duplicates above which uniqueness FAILs.
    max_duplicate_share: float
    # On-scene is used for the stage decomposition only if at least this share
    # of rides carry an independently observed, correctly ordered on-scene time.
    min_on_scene_observed_share: float
    # Minimum hourly weather coverage for the weather KPI to be published.
    min_weather_hour_coverage: float
    # Minimum rides in a pickup zone before its wait metrics are published.
    min_rides_per_zone: int
    # Minimum share of 311 complaints that must map to a taxi zone for KPI 5.
    min_311_mapping_coverage: float
    # Precipitation band edges in mm/hour. 2.5 mm/h is the conventional
    # light/moderate rain boundary (AMS glossary).
    light_rain_max_mm: float
    # Plausibility flags (flagged and counted, never deleted).
    max_plausible_trip_miles: float
    max_plausible_request_to_pickup_min: float

    log_level: str

    @classmethod
    def for_month(cls, month: str) -> "Config":
        data_dir = Path(_env("DATA_DIR", str(PROJECT_ROOT / "data")))
        return cls(
            month=month,
            data_dir=data_dir,
            output_dir=Path(_env("OUTPUT_DIR", str(PROJECT_ROOT / "output"))),
            log_dir=Path(_env("LOG_DIR", str(PROJECT_ROOT / "logs"))),
            hvfhv_url_template=_env(
                "HVFHV_URL_TEMPLATE",
                "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{month}.parquet",
            ),
            zone_lookup_url=_env(
                "ZONE_LOOKUP_URL",
                "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
            ),
            zone_shapes_url=_env(
                "ZONE_SHAPES_URL",
                "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
            ),
            nyc311_api_url=_env(
                "NYC311_API_URL", "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
            ),
            nyc311_page_size=int(_env("NYC311_PAGE_SIZE", "1000")),
            weather_api_url=_env(
                "WEATHER_API_URL", "https://archive-api.open-meteo.com/v1/archive"
            ),
            weather_latitude=float(_env("WEATHER_LATITUDE", "40.7812")),
            weather_longitude=float(_env("WEATHER_LONGITUDE", "-73.9665")),
            weather_timezone=_env("WEATHER_TIMEZONE", "America/New_York"),
            weather_variables=("temperature_2m", "precipitation", "wind_speed_10m"),
            http_timeout_seconds=float(_env("HTTP_TIMEOUT_SECONDS", "60")),
            max_retries=int(_env("MAX_RETRIES", "4")),
            retry_base_seconds=float(_env("RETRY_BASE_SECONDS", "1")),
            retry_max_wait_seconds=float(_env("RETRY_MAX_WAIT_SECONDS", "30")),
            max_excluded_share=float(_env("MAX_EXCLUDED_SHARE", "0.05")),
            max_duplicate_share=float(_env("MAX_DUPLICATE_SHARE", "0.001")),
            min_on_scene_observed_share=float(_env("MIN_ON_SCENE_OBSERVED_SHARE", "0.80")),
            min_weather_hour_coverage=float(_env("MIN_WEATHER_HOUR_COVERAGE", "0.99")),
            min_rides_per_zone=int(_env("MIN_RIDES_PER_ZONE", "1000")),
            min_311_mapping_coverage=float(_env("MIN_311_MAPPING_COVERAGE", "0.90")),
            light_rain_max_mm=float(_env("LIGHT_RAIN_MAX_MM", "2.5")),
            max_plausible_trip_miles=float(_env("MAX_PLAUSIBLE_TRIP_MILES", "100")),
            max_plausible_request_to_pickup_min=float(
                _env("MAX_PLAUSIBLE_REQUEST_TO_PICKUP_MIN", "60")
            ),
            log_level=_env("LOG_LEVEL", "INFO").upper(),
        )

    # Derived paths ---------------------------------------------------------
    @property
    def hvfhv_url(self) -> str:
        return self.hvfhv_url_template.format(month=self.month)

    @property
    def raw_hvfhv_dir(self) -> Path:
        return self.data_dir / "raw" / "hvfhv" / self.month

    @property
    def raw_weather_dir(self) -> Path:
        return self.data_dir / "raw" / "weather" / self.month

    @property
    def nyc311_dir(self) -> Path:
        return self.data_dir / "raw" / "nyc311" / self.month

    @property
    def reference_dir(self) -> Path:
        return self.data_dir / "reference"

    @property
    def partition_dir(self) -> Path:
        return self.output_dir / self.month

    def thresholds(self) -> dict:
        keys = [
            "max_excluded_share", "max_duplicate_share", "min_on_scene_observed_share",
            "min_weather_hour_coverage", "min_rides_per_zone", "min_311_mapping_coverage", "light_rain_max_mm",
            "max_plausible_trip_miles", "max_plausible_request_to_pickup_min",
        ]
        d = asdict(self)
        return {k: d[k] for k in keys}
