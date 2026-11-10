# ADR-0001: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

This is the Python edition of TransitFlow. It mirrors the Java reference exactly —
same domain, same event field names, same SAGA flow — so the architecture
decisions carry over. We keep them as ADRs so the *why* travels with the code.

## Decision

Use lightweight Markdown ADRs in `docs/adr/`, numbered sequentially. Each records
one decision with its context and consequences. They are append-only; a reversed
decision gets a new ADR that supersedes the old one.

## Consequences

- A new contributor can read the decisions in order and understand the system's
  shape before reading code.
- The trade-offs we accepted (and rejected) are explicit, not folklore.
