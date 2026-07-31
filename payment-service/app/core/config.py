from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PAYMENT_SERVICE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    database_url: str
    kafka_bootstrap_servers: str
    app_host: str
    app_port: int
    payment_provider_url: str
    payment_provider_timeout_seconds: float
    payment_provider_max_attempts: int
    payment_provider_retry_base_delay_seconds: float
    payment_provider_failure_threshold: int
    payment_provider_recovery_timeout_seconds: float

    model_config = SettingsConfigDict(
        env_file=PAYMENT_SERVICE_DIR / ".env", env_file_encoding="utf-8", extra="ignore"
    )
