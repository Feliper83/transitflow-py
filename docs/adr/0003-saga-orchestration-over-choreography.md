# ADR-0003: Orchestrated SAGA over choreography

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

A booking requires a seat reservation **and** a payment, across two services, with
no distributed transaction available (ADR-0002). We need to stay consistent when a
later step fails — e.g. the seat is held but the payment is declined. The two
common patterns are choreography (each service reacts to events) and orchestration
(one component drives the steps and issues compensations).

## Decision

Use an **orchestrated SAGA** with `booking-service` as the orchestrator. It owns
the booking state machine (`PENDING → SEAT_RESERVED → CONFIRMED | CANCELLED`) and
issues compensating actions (release the seat) when a downstream step fails.

## Consequences

- **Positive:** the business process lives in one place and is explicit, testable,
  and easy to extend. Debugging a stuck booking means reading one state machine.
- **Positive:** compensation logic is centralized and obvious.
- **Negative:** the orchestrator is a coupling hotspot; we keep it thin (it
  coordinates, it doesn't do others' work).
- **Rejected — choreography:** with only 2–3 steps it offered no benefit and would
  have scattered "what happens next" across services.
