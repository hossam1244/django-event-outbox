from django.contrib import admin

from .models import OutboxEvent


@admin.register(OutboxEvent)
class OutboxEventAdmin(admin.ModelAdmin):
    list_display = ("event_type", "aggregate", "status", "attempts", "created_at", "delivered_at")
    list_filter = ("status",)
    search_fields = ("event_type", "aggregate")
    readonly_fields = [f.name for f in OutboxEvent._meta.fields]
