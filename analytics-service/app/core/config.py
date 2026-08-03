from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ANALYTICS_SERVICE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    database_url: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    kafka_auto_offset_reset: str
    kafka_dlq_topic: str
    redis_url: str
    app_host: str
    app_port: int

    model_config = SettingsConfigDict(
        env_file=ANALYTICS_SERVICE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
