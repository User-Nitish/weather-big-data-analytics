import json
from rest_framework import serializers
from .models import WeatherEvent, AuditLog, AlertBroadcast, Grievance


class AuditLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditLog
        fields = [
            "id",
            "event_id",
            "operator_name",
            "action",
            "previous_status",
            "new_status",
            "reason",
            "timestamp",
        ]


class WeatherEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = WeatherEvent
        fields = [
            "id",
            "title",
            "description",
            "category",
            "severity",
            "latitude",
            "longitude",
            "h3_index",
            "city",
            "state",
            "source",
            "source_author",
            "source_credibility",
            "external_reference",
            "media_url",
            "media_type",
            "is_media_authentic",
            "vision_check_result",
            "trust_score",
            "nlp_confidence",
            "radar_corroborated",
            "radar_station_name",
            "radar_recorded_value",
            "verification_status",
            "operator_decision",
            "operator_notes",
            "duplicate_of_id",
            "observed_at",
            "ingested_at",
        ]


class WeatherEventCreateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=255)
    description = serializers.CharField()
    category = serializers.CharField(max_length=64)
    severity = serializers.CharField(max_length=32, default="Moderate")
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()
    city = serializers.CharField(max_length=128)
    state = serializers.CharField(max_length=128)
    source = serializers.CharField(max_length=64, default="Citizen Report")
    source_author = serializers.CharField(max_length=128, required=False, allow_null=True)
    media_url = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    media_type = serializers.CharField(max_length=32, default="none")
    observed_at = serializers.DateTimeField(required=False, allow_null=True)


class OperatorReviewRequestSerializer(serializers.Serializer):
    decision = serializers.ChoiceField(choices=["VERIFIED", "REJECTED", "QUARANTINED", "ESCALATED"])
    reason = serializers.CharField(min_length=5)
    operator_name = serializers.CharField(default="Lead Operator", required=False)
    notes = serializers.CharField(required=False, allow_null=True, allow_blank=True)


class GrievanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Grievance
        fields = [
            "id",
            "event_id",
            "complainant_name",
            "contact_email",
            "grievance_type",
            "description",
            "status",
            "resolution_note",
            "created_at",
            "resolved_at",
            "evidence_photo_url",
            "authenticity_score",
            "is_fake",
            "flagged_reason",
            "verification_breakdown",
            "satellite_concurrence",
            "vision_concurrence",
            "appeal_note",
            "is_appealed",
        ]


class GrievanceCreateSerializer(serializers.Serializer):
    event_id = serializers.CharField(max_length=64)
    complainant_name = serializers.CharField(max_length=128)
    contact_email = serializers.EmailField()
    grievance_type = serializers.CharField(max_length=64)
    description = serializers.CharField()
    evidence_photo_url = serializers.CharField()
    complainant_lat = serializers.FloatField(required=False, allow_null=True)
    complainant_lng = serializers.FloatField(required=False, allow_null=True)


class GrievanceResolveSerializer(serializers.Serializer):
    resolution_note = serializers.CharField()
    status = serializers.ChoiceField(choices=["RESOLVED", "DISMISSED"], default="RESOLVED")
    operator_name = serializers.CharField(default="Grievance Officer", required=False)


class GrievanceAppealSerializer(serializers.Serializer):
    appeal_note = serializers.CharField()


class CapAlertBroadcastSerializer(serializers.Serializer):
    event_id = serializers.CharField(max_length=64)
    headline = serializers.CharField(max_length=512)
    urgency = serializers.CharField(default="Immediate")
    severity = serializers.CharField(default="Severe")
    certainty = serializers.CharField(default="Observed")
    areas = serializers.ListField(child=serializers.CharField())
    instructions = serializers.CharField()
    target_channels = serializers.ListField(
        child=serializers.CharField(),
        default=["SMS_CELL_BROADCAST", "CAP_RSS", "WHATSAPP_EMERGENCY"]
    )
