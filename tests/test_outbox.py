from datetime import timedelta

import pytest
from django.db import transaction
from django.utils import timezone

from django_event_outbox.models import OutboxEvent
from django_event_outbox.relay import emit, handle, relay_events

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _clean_handlers():
    from django_event_outbox import relay

    saved = dict(relay._HANDLERS)
    yield
    relay._HANDLERS.clear()
    relay._HANDLERS.update(saved)


def test_emit_records_the_event():
    event = emit("order", "order.placed", {"order_id": "o1"})

    assert event.status == OutboxEvent.Status.PENDING
    assert OutboxEvent.objects.get(pk=event.pk).payload == {"order_id": "o1"}


def test_emit_rolls_back_with_the_transaction():
    with pytest.raises(RuntimeError), transaction.atomic():
        emit("order", "order.cancelled", {"order_id": "o2"})
        raise RuntimeError("business write failed")

    assert not OutboxEvent.objects.filter(event_type="order.cancelled").exists()


def test_relay_delivers_to_registered_handler():
    seen = []
    handle("order.placed")(lambda event: seen.append(event.payload))

    emit("order", "order.placed", {"order_id": "o1"})
    stats = relay_events()

    assert seen == [{"order_id": "o1"}]
    assert stats["delivered"] == 1
    event = OutboxEvent.objects.get(event_type="order.placed")
    assert event.status == OutboxEvent.Status.DELIVERED
    assert event.delivered_at is not None


def test_failed_handler_retries_then_parks_as_failed():
    calls = []

    @handle("payment.settled")
    def flaky(event):
        calls.append(1)
        raise ValueError("integration down")

    emit("payment", "payment.settled", {})

    for _ in range(5):
        relay_events()

    event = OutboxEvent.objects.get(event_type="payment.settled")
    assert event.status == OutboxEvent.Status.FAILED
    assert event.attempts == 5
    assert "integration down" in event.last_error
    assert len(calls) == 5


def test_unregistered_event_type_fails_visibly():
    emit("audit", "audit.unknown", {})

    stats = relay_events()

    event = OutboxEvent.objects.get(event_type="audit.unknown")
    assert event.status == OutboxEvent.Status.FAILED
    assert "no handler" in event.last_error
    assert stats["failed"] == 5, "one failure counted per attempt within the single relay run"


def test_stale_processing_lease_is_expired_back_to_pending():
    event = emit("order", "order.placed", {})
    OutboxEvent.objects.filter(pk=event.pk).update(
        status=OutboxEvent.Status.PROCESSING,
        created_at=timezone.now() - timedelta(hours=1),
    )

    from django_event_outbox import relay as relay_mod

    relay_mod._HANDLERS["order.placed"] = lambda e: None
    stats = relay_events()

    event.refresh_from_db()
    assert event.status == OutboxEvent.Status.DELIVERED
    assert stats["expired_leases"] == 1


def test_delivered_events_are_purged_after_retention():
    handle("order.placed")(lambda event: None)
    event = emit("order", "order.placed", {})
    relay_events()

    OutboxEvent.objects.filter(pk=event.pk).update(
        delivered_at=timezone.now() - timedelta(days=30),
    )

    stats = relay_events()

    assert stats["purged"] == 1
    assert not OutboxEvent.objects.filter(pk=event.pk).exists()


def test_relay_delivers_in_creation_order():
    order = []
    handle("order.placed")(lambda e: order.append(e.payload["n"]))
    handle("order.shipped")(lambda e: order.append(e.payload["n"]))

    emit("order", "order.placed", {"n": 1})
    emit("order", "order.shipped", {"n": 2})

    relay_events()

    assert order == [1, 2]


def test_management_command_outputs_stats(capsys):
    handle("order.placed")(lambda event: None)
    emit("order", "order.placed", {})

    from django.core.management import call_command

    call_command("relay_outbox")

    out = capsys.readouterr().out
    assert "delivered=1" in out
