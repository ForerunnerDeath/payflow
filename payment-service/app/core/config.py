from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PAYMENT_SERVICE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    database_url: str
    kafka_bootstrap_servers: str
    app_host: str
    app_port: int

    model_config = SettingsConfigDict(
        env_file=PAYMENT_SERVICE_DIR / ".env", env_file_encoding="utf-8", extra="ignore"
    )
