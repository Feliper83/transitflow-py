# ADR-0005: Idempotent consumers

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

Kafka delivers **at-least-once**: a consumer may process the same record more than
once (a rebalance before the offset is committed, or the outbox relay re-publishing
a row — ADR-0004). For side-effecting consumers this is dangerous: processing
`ProcessPaymentCommand` twice must not charge the passenger twice.

## Decision

Every consumer that causes a side effect derives an **idempotency key** from the
message (for payments, the `bookingId`) and records processed keys in its own
database. `payment-service` enforces this with a **unique constraint** on
`payments.booking_id`: a duplicate command finds the existing row and returns the
original outcome instead of charging again. `booking-service` short-circuits any
payment result for a booking already in a terminal state.

## Consequences

- **Positive:** correctness under redelivery — no double charges, no duplicate
  confirmations.
- **Negative:** idempotency state must be stored and eventually pruned; we keep it
  in each service's own database keyed by the natural business id.
- **Non-goal (named):** dead-letter queues and exactly-once semantics are discussed
  but not implemented — idempotent consumers are the pragmatic industry default.
