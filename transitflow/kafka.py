"""Thin async Kafka helpers. Messages are JSON; the key is always the booking id
as a string (so all records for one booking land on the same partition and keep
their order). Producers use idempotent delivery; consumers decode JSON to dict."""

from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


def _key_serializer(key: str | None) -> bytes | None:
    return key.encode("utf-8") if key is not None else None


async def create_string_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    """Producer that sends pre-serialized JSON strings (used by the outbox relay)
    and byte-identical values. Idempotent so a retried send is not duplicated."""
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=_key_serializer,
        value_serializer=lambda value: value.encode("utf-8"),
        enable_idempotence=True,
        acks="all",
    )
    await producer.start()
    return producer


async def create_json_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    """Producer that serializes dict payloads to JSON (used by payment-service)."""
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=_key_serializer,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        enable_idempotence=True,
        acks="all",
    )
    await producer.start()
    return producer


def create_json_consumer(
    *topics: str,
    bootstrap_servers: str,
    group_id: str,
) -> AIOKafkaConsumer:
    """Consumer that decodes JSON values to ``dict``. ``earliest`` so a freshly
    started service still sees records produced before it joined the group."""
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda data: data.decode("utf-8") if data else None,
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    )


def dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload)
