from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class TripSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://transitflow:transitflow@localhost:5433/trips"
    kafka_bootstrap_servers: str = "localhost:9092"
    port: int = 8081
