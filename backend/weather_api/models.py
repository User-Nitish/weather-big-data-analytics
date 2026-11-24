import uuid
from django.db import models
from django.utils import timezone


def generate_uuid():
    return str(uuid.uuid4())


class WeatherEvent(models.Model):
    id = models.CharField(max_length=64, primary_key=True, default=generate_uuid)
    title = models.CharField(max_length=255)
    description = models.TextField()
    category = models.CharField(max_length=64, db_index=True)
    severity = models.CharField(max_length=32, default="Moderate", db_index=True)

    # Geospatial & H3
    latitude = models.FloatField()
    longitude = models.FloatField()
    h3_index = models.CharField(max_length=32, db_index=True)
    city = models.CharField(max_length=128, db_index=True)
    state = models.CharField(max_length=128, db_index=True)

    # Provenance & Source
    source = models.CharField(max_length=64, default="Citizen Report", db_index=True)
    source_author = models.CharField(max_length=128, null=True, blank=True)
    source_credibility = models.FloatField(default=0.5)
    external_reference = models.CharField(max_length=512, null=True, blank=True)

    # Media & Multimodal Authenticity
    media_url = models.TextField(null=True, blank=True)
    media_type = models.CharField(max_length=32, default="none")
    is_media_authentic = models.BooleanField(default=True)
    vision_check_result = models.TextField(null=True, blank=True)

    # AI Truth & Corroboration
    trust_score = models.FloatField(default=50.0)
    nlp_confidence = models.FloatField(default=0.8)
    radar_corroborated = models.BooleanField(default=False)
    radar_station_name = models.CharField(max_length=255, null=True, blank=True)
    radar_recorded_value = models.FloatField(null=True, blank=True)

    # Workflow & Moderation
    verification_status = models.CharField(max_length=32, default="PENDING_REVIEW", db_index=True)
    operator_decision = models.CharField(max_length=32, default="UNREVIEWED")
    operator_notes = models.TextField(null=True, blank=True)
    duplicate_of_id = models.CharField(max_length=64, null=True, blank=True)

    # Timestamps
    observed_at = models.DateTimeField(default=timezone.now, db_index=True)
    ingested_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "weather_events"
        ordering = ["-observed_at"]

    def __str__(self):
        return f"{self.title} ({self.city}, {self.category})"


class AuditLog(models.Model):
    event = models.ForeignKey(WeatherEvent, on_delete=models.CASCADE, related_name="audits", db_column="event_id")
    operator_name = models.CharField(max_length=128, default="Chief Operator")
    action = models.CharField(max_length=64)
    previous_status = models.CharField(max_length=32, null=True, blank=True)
    new_status = models.CharField(max_length=32)
    reason = models.TextField()
    timestamp = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "audit_logs"
        ordering = ["timestamp"]

    def __str__(self):
        return f"Audit: {self.action} on {self.event_id}"


class AlertBroadcast(models.Model):
    id = models.CharField(max_length=64, primary_key=True)
    event = models.ForeignKey(WeatherEvent, on_delete=models.CASCADE, related_name="alert_broadcasts", db_column="event_id")
    headline = models.CharField(max_length=512)
    urgency = models.CharField(max_length=32, default="Immediate")
    severity = models.CharField(max_length=32, default="Severe")
    certainty = models.CharField(max_length=32, default="Observed")
    areas = models.TextField()  # JSON string
    instructions = models.TextField()
    target_channels = models.TextField()  # JSON string
    dispatched_by = models.CharField(max_length=128, default="Senior Disaster Operations Chief")
    sent_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "alert_broadcasts"
        ordering = ["-sent_at"]

    def __str__(self):
        return f"Alert: {self.headline} ({self.id})"


class Grievance(models.Model):
    id = models.CharField(max_length=64, primary_key=True, default=generate_uuid)
    event = models.ForeignKey(WeatherEvent, on_delete=models.CASCADE, related_name="grievances", db_column="event_id")
    complainant_name = models.CharField(max_length=128)
    contact_email = models.CharField(max_length=128)
    grievance_type = models.CharField(max_length=64)
    description = models.TextField()
    status = models.CharField(max_length=32, default="OPEN", db_index=True)
    resolution_note = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    resolved_at = models.DateTimeField(null=True, blank=True)

    # 55-45 AI Verification & Fraud Detection Fields
    evidence_photo_url = models.TextField(null=True, blank=True)
    authenticity_score = models.FloatField(default=0.0)
    is_fake = models.BooleanField(default=False)
    flagged_reason = models.TextField(null=True, blank=True)
    verification_breakdown = models.TextField(null=True, blank=True)
    satellite_concurrence = models.FloatField(default=0.0)
    vision_concurrence = models.FloatField(default=0.0)
    appeal_note = models.TextField(null=True, blank=True)
    is_appealed = models.BooleanField(default=False)

    class Meta:
        db_table = "grievances"
        ordering = ["-created_at"]

    def __str__(self):
        return f"Grievance: {self.grievance_type} ({self.complainant_name})"
