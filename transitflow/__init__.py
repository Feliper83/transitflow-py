"""TransitFlow (Python) — an event-driven transit ticketing platform.

Three services — trip, payment, booking (SAGA orchestrator) — that stay
consistent without distributed transactions using an orchestrated SAGA with
compensation, the transactional outbox pattern, and idempotent consumers.
"""

__version__ = "1.0.0"
