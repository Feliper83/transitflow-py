"""payment-service entrypoint. It is a Kafka worker, but we wrap it in a tiny
FastAPI app so it exposes a ``/health`` endpoint and starts/stops the consumer on
the ASGI lifespan — giving it a uniform uvicorn entrypoint and a healthcheck."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from transitflow.db import create_engine, create_session_factory
from transitflow.logging_config import configure_logging
from transitflow.metrics import setup_metrics
from transitflow.payment.config import PaymentSettings
from transitflow.payment.consumer import PaymentConsumer
from transitflow.payment.models import Base
from transitflow.payment.repository import SqlAlchemyPaymentUnitOfWork
from transitflow.payment.service import PaymentService


def create_app(settings: PaymentSettings | None = None) -> FastAPI:
    settings = settings or PaymentSettings()
    configure_logging("payment-service")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_engine(settings.database_url)
        session_factory = create_session_factory(engine)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        service = PaymentService(
            lambda: SqlAlchemyPaymentUnitOfWork(session_factory),
            settings.decline_above,
        )
        consumer = PaymentConsumer(service, settings.kafka_bootstrap_servers)
        await consumer.start()
        app.state.consumer = consumer
        yield
        await consumer.stop()
        await engine.dispose()

    app = FastAPI(title="TransitFlow payment-service", lifespan=lifespan)
    setup_metrics(app)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "UP"}

    return app


app = create_app()
