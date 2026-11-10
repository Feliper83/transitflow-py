from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BookingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://transitflow:transitflow@localhost:5434/bookings"
    kafka_bootstrap_servers: str = "localhost:9092"
    port: int = 8082
    trip_service_url: str = Field(
        default="http://localhost:8081",
        validation_alias=AliasChoices("trip_service_url", "TRIP_SERVICE_URL"),
    )
    outbox_poll_interval_seconds: float = 2.0
