# django-event-outbox

**Transactional outbox for Django** — emit events in the same transaction as
your writes, relay them reliably with retries, locking, and lease expiry.

[![CI](https://github.com/hossam1244/django-event-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/hossam1244/django-event-outbox/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/badge/pypi-0.1.0-blue)](https://pypi.org/project/django-event-outbox/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Why

"Update the database **and** publish to Kafka/send the webhook" is two writes
to two systems — and when the second one fails, the two disagree forever
(dual-write problem). The transactional outbox fixes it with one extra table:

1. **Emit inside the transaction.** `emit(...)` writes the event to the
   outbox table in the same DB transaction as your state change. Both commit,
   or neither does.
2. **Relay separately.** A background drain (`manage.py relay_outbox`, cron
   or Celery beat) delivers events to handlers — with retries, attempt
   limits, and double-delivery protection.

## Guarantees

| Property | How |
|---|---|
| Atomic with your writes | `emit` runs inside your `transaction.atomic()` — rollback takes the event with it |
| No double delivery | events are claimed `PENDING → PROCESSING` under `select_for_update(skip_locked=True)`; concurrent relays are safe |
| Crashed relay self-heals | stale `PROCESSING` leases (default 10 min) expire back to `PENDING` |
| Failures are visible | after 5 attempts an event parks as `FAILED` with the last error — queryable, never silently dropped |
| FIFO per drain | events deliver in creation order |
| Retention hygiene | delivered events older than 7 days are purged by the relay |

## Usage

```python
# settings.py
INSTALLED_APPS = [..., "django_event_outbox"]
# migrate → creates the outbox table

# Your domain code — the event is atomic with the order:
from django.db import transaction
from django_event_outbox import emit, handle


@handle("order.placed")
def on_order_placed(event):
    webhooks.deliver(event.payload)  # your side effect
    # or push to Kafka / SNS / a websocket group…


def place_order(cart):
    with transaction.atomic():
        order = Order.objects.create.from_cart(cart)
        emit("order", "order.placed", {"order_id": order.pk, "total": str(order.total)})
```

Drain (cron/Celery beat, every ~30s):

```bash
python manage.py relay_outbox            # prints: delivered=3 failed=0 expired_leases=0 purged=12
python manage.py relay_outbox --limit 500
```

Or from code: `relay_events(limit=100)` returns the same counters for your
monitoring.

## Testing

9 tests: transactional atomicity (rollback removes the event), handler
delivery + status transition, retry-then-FAILED with attempt accounting,
unregistered-type failure visibility, stale-lease expiry, retention purge,
FIFO ordering, and the management command output.

```bash
pip install -e . && pytest
```

## License

[MIT](LICENSE)
