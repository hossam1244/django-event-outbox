"""Emitting and relaying.

`emit` is called *inside* the transaction that changes state; `relay_events`
is the drain — called from a management command, Celery beat, or cron.
"""

import logging
from collections.abc import Callable
from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from django_event_outbox.models import OutboxEvent

logger = logging.getLogger(__name__)

#: Handlers registered per event type: ``@handle("order.placed")``.
_HANDLERS: dict[str, Callable[[OutboxEvent], None]] = {}

#: A handler that raises marks the attempt failed; after MAX_ATTEMPTS the
#: event is parked as FAILED — visible, retryable, never silently dropped.
MAX_ATTEMPTS = 5
#: Events older than this are considered abandoned by a crashed relay and
#: returned to PENDING (processing lease expiry).
LEASE = timedelta(minutes=10)
#: Events delivered before this cutoff are purged by `relay_events`.
RETENTION = timedelta(days=7)


def handle(*event_types: str):
    """Decorator registering a handler for one or more event types."""

    def register(func):
        for event_type in event_types:
            _HANDLERS[event_type] = func
        return func

    return register


def emit(aggregate: str, event_type: str, payload: dict) -> OutboxEvent:
    """Records an event on the outbox table.

    Call this inside the transaction that performs the state change — the
    event commits (or rolls back) together with it.
    """
    if event_type not in _HANDLERS:
        logger.warning("django-event-outbox: no handler registered for %r", event_type)
    return OutboxEvent.objects.create(aggregate=aggregate, event_type=event_type, payload=payload)


def _expire_stale_leases(now) -> int:
    stale = OutboxEvent.objects.filter(
        status=OutboxEvent.Status.PROCESSING, created_at__lt=now - LEASE
    )
    count = stale.count()
    if count:
        stale.update(status=OutboxEvent.Status.PENDING)
        logger.warning("django-event-outbox: expired %d stale processing lease(s)", count)
    return count


def _purge_delivered(now) -> int:
    delivered_before = now - RETENTION
    old = OutboxEvent.objects.filter(
        status=OutboxEvent.Status.DELIVERED, delivered_at__lt=delivered_before
    )
    count = old.count()
    if count:
        old.delete()
    return count


def relay_events(limit: int = 100) -> dict[str, int]:
    """Drains pending events through their registered handlers.

    Safe to run concurrently: each event is claimed atomically
    (PENDING → PROCESSING inside a transaction with select_for_update),
    so two relays can never double-deliver.

    Returns counters for monitoring: ``{"delivered": n, "failed": n,
    "expired_leases": n, "purged": n}``.
    """
    now = timezone.now()
    stats = {
        "delivered": 0,
        "failed": 0,
        "expired_leases": _expire_stale_leases(now),
        "purged": _purge_delivered(now),
    }

    while True:
        with transaction.atomic():
            claimed = (
                OutboxEvent.objects.select_for_update(skip_locked=True)
                .filter(status=OutboxEvent.Status.PENDING)
                .order_by("created_at")[:limit]
            )
            events = list(claimed)
            if not events:
                return stats
            OutboxEvent.objects.filter(pk__in=[event.pk for event in events]).update(
                status=OutboxEvent.Status.PROCESSING
            )

        for event in events:
            handler = _HANDLERS.get(event.event_type)
            event.attempts += 1
            try:
                if handler is None:
                    raise RuntimeError(f"no handler registered for {event.event_type!r}")
                handler(event)
            except Exception as error:  # noqa: BLE001 — handlers are user code
                event.last_error = f"{type(error).__name__}: {error}"
                if event.attempts >= MAX_ATTEMPTS:
                    event.status = OutboxEvent.Status.FAILED
                else:
                    event.status = OutboxEvent.Status.PENDING
                event.save(update_fields=["status", "attempts", "last_error"])
                stats["failed"] += 1
                logger.warning(
                    "django-event-outbox: %s attempt %d failed: %s",
                    event.event_type,
                    event.attempts,
                    error,
                )
            else:
                event.status = OutboxEvent.Status.DELIVERED
                event.delivered_at = timezone.now()
                event.last_error = ""
                event.save(update_fields=["status", "attempts", "delivered_at", "last_error"])
                stats["delivered"] += 1
