"""trip-service FastAPI application: trip catalogue plus synchronous seat
reserve/release endpoints called by the booking SAGA."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse

from transitflow.db import create_engine, create_session_factory
from transitflow.logging_config import configure_logging
from transitflow.metrics import setup_metrics
from transitflow.trip.config import TripSettings
from transitflow.trip.models import Base
from transitflow.trip.repository import SqlAlchemyTripUnitOfWork
from transitflow.trip.schemas import ReservationResult, ReserveRequest, TripView
from transitflow.trip.seed import seed_trips
from transitflow.trip.service import TripNotFoundError, TripService


def build_service(settings: TripSettings) -> tuple[TripService, object]:
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    service = TripService(lambda: SqlAlchemyTripUnitOfWork(session_factory))
    return service, (engine, session_factory)


def create_app(settings: TripSettings | None = None) -> FastAPI:
    settings = settings or TripSettings()
    configure_logging("trip-service")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service, (engine, session_factory) = build_service(settings)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await seed_trips(session_factory)
        app.state.service = service
        yield
        await engine.dispose()

    app = FastAPI(title="TransitFlow trip-service", lifespan=lifespan)
    setup_metrics(app)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "UP"}

    @app.get("/api/trips", response_model=list[TripView])
    async def list_trips():
        trips = await app.state.service.list_trips()
        return [TripView.model_validate(t) for t in trips]

    @app.get("/api/trips/{trip_id}", response_model=TripView)
    async def get_trip(trip_id: int):
        try:
            trip = await app.state.service.get_trip(trip_id)
        except TripNotFoundError:
            raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
        return TripView.model_validate(trip)

    @app.post("/api/trips/{trip_id}/reservations")
    async def reserve(trip_id: int, request: ReserveRequest):
        try:
            result: ReservationResult = await app.state.service.reserve(
                trip_id, request.booking_ref, request.seat_count
            )
        except TripNotFoundError:
            raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
        status = 200 if result.reserved else 409
        return JSONResponse(status_code=status, content=result.model_dump(by_alias=True))

    @app.post("/api/trips/reservations/{booking_ref}/release", status_code=204)
    async def release(booking_ref: str) -> Response:
        await app.state.service.release(booking_ref)
        return Response(status_code=204)

    return app


app = create_app()
