"""Cross-cutting primitives shared by every service: the camelCase schema base
and a money type that serializes as a JSON number (to match the Java edition's
``BigDecimal`` wire format and keep event field names consistent)."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, PlainSerializer
from pydantic.alias_generators import to_camel

# A Decimal in Python, a JSON number on the wire (e.g. 120.00 -> 120.0), never a
# string. Keeps monetary precision in-process while matching the Java payloads.
Money = Annotated[
    Decimal,
    PlainSerializer(lambda value: float(value), return_type=float, when_used="json"),
]


class CamelModel(BaseModel):
    """Base schema whose JSON uses camelCase field names (``bookingId``,
    ``correlationId``, …) while Python code uses snake_case. Event and REST field
    names therefore stay identical across all three services."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )
