import os
import logging
from typing import List

logger = logging.getLogger(__name__)

# --- Pydantic Fallback ---
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict

    HAS_PYDANTIC = True
except ImportError:
    try:
        from pydantic import BaseSettings

        HAS_PYDANTIC = True
        SettingsConfigDict = dict  
    except ImportError:
        HAS_PYDANTIC = False
        logger.warning(
            "Pydantic not found. Using minimal fallback configuration class."
        )

        class BaseSettings:
            """Minimal fallback if Pydantic is missing."""

            def __init__(self, **kwargs):
                for key, default_value in kwargs.items():
                    env_val = os.environ.get(key.upper())
                    setattr(
                        self, key, env_val if env_val is not None else default_value
                    )


class Settings(BaseSettings):
    """
    Global application settings.
    Default values are provided for local development.
    """

    # --- API Category ---
    APP_NAME: str = "NYC Taxi Real-Time Analytics API"
    APP_DESCRIPTION: str = "FastAPI REST API for Taxi Analytics"
    APP_VERSION: str = "1.0.0"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    ENVIRONMENT: str = "development"
    CORS_ORIGINS: List[str] = ["*"]  # Harden in production

    # Auth Category
    #Override this via environment variable in production
    API_KEY: str = (
        "huynz-super-secret-key-2026"  # Default; override via API_KEY env var
    )
    API_KEY_NAME: str = "X-API-Key"

    # PostgreSQL (OLTP) 
    PG_HOST: str = "localhost"
    PG_PORT: int = 5432
    PG_DB: str = "taxi_db"
    PG_USER: str = "admin"
    PG_PASSWORD: str = "admin"

    # Airflow DB 
    AIRFLOW_DB_HOST: str = "localhost"
    AIRFLOW_DB_PORT: int = 5433
    AIRFLOW_DB_NAME: str = "airflow"
    AIRFLOW_DB_USER: str = "airflow"
    AIRFLOW_DB_PASSWORD: str = "airflow"

    # ClickHouse
    CH_HOST: str = "localhost"
    CH_PORT: int = 8123  # HTTP API port
    CH_USER: str = "admin"
    CH_PASSWORD: str = "admin"
    CH_DB: str = "lakehouse"

    # MinIO (Data Lake)
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "admin"
    MINIO_SECRET_KEY: str = "admin12345"
    MINIO_BUCKET: str = "lakehouse"
    MINIO_SECURE: bool = False  # Set to True in production

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    # Connection Pool Configuration
    # ClickHouse Pool
    CH_POOL_SIZE: int = 20  # Base pool size
    CH_MAX_OVERFLOW: int = 10  # Extra connections during burst
    CH_POOL_TIMEOUT: int = 30  # Seconds to wait for connection
    CH_POOL_RECYCLE: int = 3600  # Recycle connections after 1 hour
    CH_POOL_PRE_PING: bool = True  # Validate connections before use

    # PostgreSQL Pool
    PG_POOL_MIN: int = 2  # Minimum connections in pool
    PG_POOL_MAX: int = 20  # Maximum connections in pool

    if HAS_PYDANTIC:
        # Pydantic config
        model_config = SettingsConfigDict(
            env_file=".env", env_file_encoding="utf-8", extra="ignore"
        )

    # Derived URLs
    @property
    def database_url(self) -> str:
        """Constructs the SQLAlchemy URL for the main PostgreSQL database."""
        return f"postgresql://{self.PG_USER}:{self.PG_PASSWORD}@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DB}"

    @property
    def airflow_database_url(self) -> str:
        """Constructs the SQLAlchemy URL for the Airflow database."""
        return f"postgresql://{self.AIRFLOW_DB_USER}:{self.AIRFLOW_DB_PASSWORD}@{self.AIRFLOW_DB_HOST}:{self.AIRFLOW_DB_PORT}/{self.AIRFLOW_DB_NAME}"

    @property
    def clickhouse_url(self) -> str:
        """Constructs the ClickHouse connection string."""
        return f"clickhouse://{self.CH_USER}:{self.CH_PASSWORD}@{self.CH_HOST}:{self.CH_PORT}/{self.CH_DB}"


# Global singleton instance to be imported across the app
settings = Settings()
