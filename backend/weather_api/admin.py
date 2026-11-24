from django.contrib import admin
from .models import WeatherEvent, AuditLog, AlertBroadcast, Grievance


@admin.register(WeatherEvent)
class WeatherEventAdmin(admin.ModelAdmin):
    list_display = ("id", "title", "city", "state", "category", "severity", "verification_status", "trust_score", "observed_at")
    list_filter = ("category", "severity", "verification_status", "operator_decision", "state")
    search_fields = ("title", "description", "city", "state", "id")
    ordering = ("-observed_at",)


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ("id", "event", "operator_name", "action", "new_status", "timestamp")
    list_filter = ("action", "new_status")
    search_fields = ("event__id", "operator_name", "reason")
    ordering = ("-timestamp",)


@admin.register(AlertBroadcast)
class AlertBroadcastAdmin(admin.ModelAdmin):
    list_display = ("id", "headline", "urgency", "severity", "certainty", "dispatched_by", "sent_at")
    list_filter = ("urgency", "severity", "certainty")
    search_fields = ("id", "headline", "instructions")
    ordering = ("-sent_at",)


@admin.register(Grievance)
class GrievanceAdmin(admin.ModelAdmin):
    list_display = ("id", "event", "complainant_name", "grievance_type", "status", "authenticity_score", "is_fake", "created_at")
    list_filter = ("status", "grievance_type", "is_fake", "is_appealed")
    search_fields = ("id", "complainant_name", "contact_email", "description")
    ordering = ("-created_at",)
