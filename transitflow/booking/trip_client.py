"""Synchronous (HTTP) client for trip-service: fetch trip price, reserve a seat,
and release it (the SAGA compensation). The correlation id is propagated via the
``X-Correlation-Id`` header so a booking is traceable across services."""

from __future__ import annotations

import logging

import httpx

from transitflow.booking.schemas import TripView
from transitflow.logging_config import correlation_id

log = logging.getLogger(__name__)


class TripServiceError(Exception):
    pass


class TripClient:
    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(base_url=self._base_url, timeout=5.0)

    def _headers(self) -> dict[str, str]:
        return {"X-Correlation-Id": correlation_id.get()}

    async def get_trip(self, trip_id: int) -> TripView:
        response = await self._client.get(
            f"/api/trips/{trip_id}", headers=self._headers()
        )
        response.raise_for_status()
        return TripView.model_validate(response.json())

    async def reserve(self, trip_id: int, booking_ref: str, seat_count: int) -> bool:
        """Returns True if the seat was held, False if the trip had no capacity
        (HTTP 409). Any other non-2xx status is an error."""
        response = await self._client.post(
            f"/api/trips/{trip_id}/reservations",
            json={"bookingRef": booking_ref, "seatCount": seat_count},
            headers=self._headers(),
        )
        if response.is_success:
            return True
        if response.status_code == 409:
            return False
        raise TripServiceError(
            f"trip-service reservation failed: {response.status_code}"
        )

    async def release(self, booking_ref: str) -> None:
        response = await self._client.post(
            f"/api/trips/reservations/{booking_ref}/release",
            headers=self._headers(),
        )
        if response.is_error:
            log.warning("Seat release for %s returned %s", booking_ref, response.status_code)

    async def aclose(self) -> None:
        await self._client.aclose()
