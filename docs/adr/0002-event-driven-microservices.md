# ADR-0002: Event-driven microservices with a database per service

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

A booking touches three bounded contexts — trips, bookings, payments. We want
them to evolve, deploy, and scale independently, which rules out a shared
database and the implicit coupling it creates.

## Decision

Split into three services, each owning its own PostgreSQL database. Services
integrate only through well-defined APIs (synchronous HTTP where an immediate
answer is required) and asynchronous events on Apache Kafka. No service reads
another service's tables.

Because there is no shared database transaction spanning services, cross-service
consistency is achieved with a SAGA (ADR-0003), reliable publishing with the
transactional outbox (ADR-0004), and idempotent consumers (ADR-0005).

## Consequences

- **Positive:** independent deployability and clear ownership boundaries.
- **Positive:** failure isolation — a payment outage doesn't take down trip search.
- **Negative:** no cross-service ACID transaction; we must engineer consistency
  explicitly. The rest of the ADRs are the price of this decision.
