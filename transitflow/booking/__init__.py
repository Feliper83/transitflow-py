"""booking-service — the SAGA orchestrator. Owns the booking lifecycle
(PENDING -> SEAT_RESERVED -> CONFIRMED | CANCELLED), the transactional outbox,
and the compensation logic."""
