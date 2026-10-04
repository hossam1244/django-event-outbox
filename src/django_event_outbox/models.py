"""The outbox table: events written in the same transaction as domain data."""

import uuid

from django.db import models
from django.utils import timezone


class OutboxEvent(models.Model):
    """One pending (or delivered) domain event.

    Written inside the same transaction as the state change it describes —
    the dual-write problem disappears: either both commit, or neither does.
    """

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        PROCESSING = "processing", "Processing"
        DELIVERED = "delivered", "Delivered"
        FAILED = "failed", "Failed (attempts exhausted)"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    aggregate = models.CharField(
        max_length=100,
        help_text="Entity the event is about, e.g. 'order'.",
    )
    event_type = models.CharField(
        max_length=100,
        help_text="What happened, e.g. 'order.placed'.",
    )
    payload = models.JSONField(default=dict)
    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDING)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    delivered_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["aggregate", "-created_at"]),
        ]
        ordering = ("created_at",)

    def __str__(self) -> str:
        return f"{self.event_type} ({self.status})"
