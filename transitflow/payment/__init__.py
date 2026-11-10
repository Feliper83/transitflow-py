"""payment-service — consumes ProcessPaymentCommand, charges idempotently (unique
on bookingId), and emits PaymentResultEvent (COMPLETED / FAILED)."""
