"""
=============================================================
  CONFIG / SETTINGS.PY  —  The Brain's Memory
=============================================================

WHAT IS THIS?
  Think of this file like the "settings" on your phone.
  It stores all the important options and passwords that
  the rest of the project needs to work.

WHY DO WE KEEP SETTINGS SEPARATE?
  If you bake the password into 50 different files and
  the password changes, you'd have to fix 50 files.
  Here you fix ONE place and everything updates.
=============================================================
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

# Load a ".env" file if it exists (keeps secrets out of code)
load_dotenv()


@dataclass
class DatabaseConfig:
    """
    DATABASE CONFIG — Where our data lives permanently.

    A database is like a giant, super-organised filing cabinet.
    PostgreSQL is the brand of filing cabinet we're using.
    """
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    name: str = os.getenv("DB_NAME", "etl_pipeline")
    user: str = os.getenv("DB_USER", "etl_user")
    password: str = os.getenv("DB_PASSWORD", "etl_password")

    @property
    def url(self) -> str:
        """Build a connection string — like writing the full address on an envelope."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def url_sqlite(self) -> str:
        """SQLite fallback — a simpler file-based database, great for testing."""
        return "sqlite:///etl_pipeline.db"


@dataclass
class RedisConfig:
    """
    REDIS CONFIG — The message board.

    Redis is like a bulletin board where workers leave notes
    for each other. Celery (our task scheduler) uses it.
    """
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))

    @property
    def url(self) -> str:
        return f"redis://{self.host}:{self.port}/{self.db}"


@dataclass
class PipelineConfig:
    """
    PIPELINE CONFIG — The conveyor belt settings.

    A pipeline is like a factory conveyor belt:
      raw materials go in one end → finished product comes out the other.
    """
    # How many rows to process at once (like reading a book 100 pages at a time)
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))

    # Maximum number of workers (like how many factory workers are on shift)
    max_workers: int = int(os.getenv("MAX_WORKERS", "4"))

    # How many times to retry if something fails
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))

    # Seconds to wait between retries (so we don't hammer a broken server)
    retry_delay: int = int(os.getenv("RETRY_DELAY", "5"))

    # Where to save raw data files
    raw_data_path: str = "data/raw"

    # Where to save cleaned data files
    processed_data_path: str = "data/processed"

    # Where to save data that failed validation
    failed_data_path: str = "data/failed"

    # Supported input file types
    supported_formats: list = field(default_factory=lambda: ["csv", "json", "parquet", "xlsx"])


@dataclass
class MonitoringConfig:
    """
    MONITORING CONFIG — The factory health display.

    Just like a car dashboard shows speed/fuel/warnings,
    monitoring shows us how healthy our pipeline is.
    """
    enable_metrics: bool = True
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: str = "logs/pipeline.log"
    alert_on_failure: bool = True
    metrics_file: str = "logs/metrics.json"


# ── MASTER CONFIG ──────────────────────────────────────────
# One object that holds ALL configs. Import this anywhere.
class Config:
    """
    THE MASTER SETTINGS BOOK.

    Import this class anywhere and you get every setting:
        from config.settings import Config
        print(Config.database.host)
    """
    database = DatabaseConfig()
    redis = RedisConfig()
    pipeline = PipelineConfig()
    monitoring = MonitoringConfig()

    # Version stamp — useful when deploying updates
    VERSION = "2.0.0"
    PROJECT_NAME = "Advanced ETL Pipeline"
