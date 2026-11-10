from __future__ import annotations

from decimal import Decimal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PaymentSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://transitflow:transitflow@localhost:5435/payments"
    kafka_bootstrap_servers: str = "localhost:9092"
    port: int = 8083
    # Payments above this amount are deterministically declined, so the SAGA
    # compensation path can be demonstrated on demand.
    decline_above: Decimal = Field(
        default=Decimal("500.00"),
        validation_alias=AliasChoices(
            "decline_above", "TRANSITFLOW_PAYMENT_DECLINE_ABOVE"
        ),
    )
