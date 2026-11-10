"""booking-service FastAPI application — the SAGA orchestrator. On lifespan it
creates tables, starts the outbox relay and the payment-events consumer, and
wires the trip-service HTTP client."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse

from transitflow.booking.config import BookingSettings
from transitflow.booking.consumer import PaymentResultConsumer
from transitflow.booking.models import Base
from transitflow.booking.orchestrator import BookingOrchestrator
from transitflow.booking.outbox_relay import OutboxRelay
from transitflow.booking.repository import SqlAlchemyBookingUnitOfWork
from transitflow.booking.schemas import BookingView, CreateBookingRequest
from transitflow.booking.store import BookingNotFoundError, BookingStore
from transitflow.booking.trip_client import TripClient
from transitflow.db import create_engine, create_session_factory
from transitflow.logging_config import configure_logging, set_correlation_id
from transitflow.metrics import setup_metrics


def create_app(settings: BookingSettings | None = None) -> FastAPI:
    settings = settings or BookingSettings()
    configure_logging("booking-service")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_engine(settings.database_url)
        session_factory = create_session_factory(engine)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        def uow_factory():
            return SqlAlchemyBookingUnitOfWork(session_factory)

        trip_client = TripClient(settings.trip_service_url)
        store = BookingStore(uow_factory)
        orchestrator = BookingOrchestrator(store, trip_client)

        relay = OutboxRelay(
            uow_factory,
            settings.kafka_bootstrap_servers,
            settings.outbox_poll_interval_seconds,
        )
        consumer = PaymentResultConsumer(orchestrator, settings.kafka_bootstrap_servers)
        await relay.start()
        await consumer.start()

        app.state.orchestrator = orchestrator
        yield

        await consumer.stop()
        await relay.stop()
        await trip_client.aclose()
        await engine.dispose()

    app = FastAPI(title="TransitFlow booking-service", lifespan=lifespan)
    setup_metrics(app)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "UP"}

    @app.post("/api/bookings", status_code=202)
    async def create_booking(
        request: CreateBookingRequest,
        x_correlation_id: str | None = Header(default=None),
    ) -> JSONResponse:
        corr_id = x_correlation_id or str(uuid.uuid4())
        set_correlation_id(corr_id)
        try:
            booking = await app.state.orchestrator.book(
                request.trip_id, request.passenger_name, request.seat_count, corr_id
            )
            view = BookingView.model_validate(booking)
            return JSONResponse(status_code=202, content=view.model_dump(by_alias=True))
        finally:
            set_correlation_id(None)

    @app.get("/api/bookings/{booking_id}", response_model=BookingView)
    async def get_booking(booking_id: int):
        try:
            booking = await app.state.orchestrator.get(booking_id)
        except BookingNotFoundError:
            raise HTTPException(status_code=404, detail=f"Booking {booking_id} not found")
        return BookingView.model_validate(booking)

    return app


app = create_app()
