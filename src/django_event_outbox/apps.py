from django.apps import AppConfig


class DjangoEventOutboxConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_event_outbox"
    verbose_name = "Django Event Outbox"
