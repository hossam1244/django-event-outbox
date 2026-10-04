"""Drain the outbox: `python manage.py relay_outbox [--limit N]`.

Wire to cron/Celery beat — e.g. every 30 seconds. Concurrent runs are safe
(events are claimed with select_for_update + skip_locked).
"""

from django.core.management.base import BaseCommand

from django_event_outbox.relay import relay_events


class Command(BaseCommand):
    help = "Relay pending outbox events to their registered handlers."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=100, help="Batch size per claim round.")

    def handle(self, *args, **options):
        stats = relay_events(limit=options["limit"])
        self.stdout.write(
            f"delivered={stats['delivered']} failed={stats['failed']} "
            f"expired_leases={stats['expired_leases']} purged={stats['purged']}"
        )
