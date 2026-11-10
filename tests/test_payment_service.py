"""payment-service: idempotency on booking id + the decline-above threshold."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.fakes import payment_uow_factory
from transitflow.payment.models import Payment, PaymentStatus
from transitflow.payment.schemas import ProcessPaymentCommand
from transitflow.payment.service import PaymentService


def make_service(payments: dict[int, Payment]) -> PaymentService:
    return PaymentService(payment_uow_factory(payments), Decimal("500.00"))


async def test_approves_payment_within_limit():
    payments: dict[int, Payment] = {}
    service = make_service(payments)

    result = await service.process(
        ProcessPaymentCommand(booking_id=1, amount=Decimal("120.00"), correlation_id="corr-1")
    )

    assert result.status == "COMPLETED"
    assert result.booking_id == 1
    assert 1 in payments  # a charge was recorded


async def test_declines_payment_above_limit():
    payments: dict[int, Payment] = {}
    service = make_service(payments)

    result = await service.process(
        ProcessPaymentCommand(booking_id=2, amount=Decimal("600.00"), correlation_id="corr-2")
    )

    assert result.status == "FAILED"
    assert "limit" in (result.reason or "")


async def test_amount_equal_to_threshold_is_approved():
    # Decline is strictly greater-than, so exactly 500.00 still completes.
    payments: dict[int, Payment] = {}
    service = make_service(payments)

    result = await service.process(
        ProcessPaymentCommand(booking_id=5, amount=Decimal("500.00"), correlation_id="corr-5")
    )

    assert result.status == "COMPLETED"


async def test_is_idempotent_for_duplicate_command():
    existing = Payment(3, Decimal("120.00"), PaymentStatus.COMPLETED, None)
    payments = {3: existing}
    service = make_service(payments)

    result = await service.process(
        ProcessPaymentCommand(booking_id=3, amount=Decimal("120.00"), correlation_id="corr-3")
    )

    assert result.status == "COMPLETED"
    # No second charge: the stored Payment object is unchanged (same identity).
    assert payments[3] is existing


@pytest.mark.parametrize(
    "amount,expected",
    [
        (Decimal("0.00"), "COMPLETED"),
        (Decimal("499.99"), "COMPLETED"),
        (Decimal("500.01"), "FAILED"),
        (Decimal("10000.00"), "FAILED"),
    ],
)
async def test_threshold_boundaries(amount, expected):
    service = make_service({})
    result = await service.process(
        ProcessPaymentCommand(booking_id=99, amount=amount, correlation_id="c")
    )
    assert result.status == expected
