from django.urls import path
from . import views

urlpatterns = [
    # Events
    path("events/", views.get_events, name="get_events"),
    path("events/clusters/h3", views.get_h3_clusters, name="get_h3_clusters"),
    path("events/geo/reverse", views.reverse_geocode, name="reverse_geocode"),
    path("events/geo/detect-ip", views.detect_location_by_ip, name="detect_location_by_ip"),
    path("events/<str:event_id>", views.get_event_detail, name="get_event_detail"),

    # Ingestion
    path("ingest/citizen", views.submit_citizen_report, name="submit_citizen_report"),
    path("ingest/sync-live-telemetry", views.sync_live_telemetry, name="sync_live_telemetry"),
    path("ingest/sync-twitter", views.sync_twitter_feed, name="sync_twitter_feed"),
    path("ingest/sync-serpapi", views.sync_serpapi_feed, name="sync_serpapi_feed"),

    # Review
    path("review/queue", views.get_review_queue, name="get_review_queue"),
    path("review/seed-pending", views.seed_pending_review_events, name="seed_pending_review_events"),
    path("review/<str:event_id>", views.apply_operator_decision, name="apply_operator_decision"),

    # Grievances
    path("grievances/", views.grievances_collection, name="grievances_collection"),
    path("grievances/<str:grievance_id>/resolve", views.resolve_grievance, name="resolve_grievance"),
    path("grievances/<str:grievance_id>/appeal", views.appeal_grievance, name="appeal_grievance"),

    # Analytics & Big Data Spark Pipeline
    path("analytics/summary", views.get_analytics_summary, name="get_analytics_summary"),
    path("analytics/charts", views.get_chart_analytics, name="get_chart_analytics"),
    path("analytics/spark", views.get_spark_analytics, name="get_spark_analytics"),
    path("analytics/spark/trigger", views.trigger_spark_job, name="trigger_spark_job"),

    # Alerts
    path("alerts/broadcast-cap", views.broadcast_cap_alert, name="broadcast_cap_alert"),
    path("alerts/history", views.get_alert_history, name="get_alert_history"),

    # Export
    path("export/pdf/<str:event_id>", views.download_incident_brief_pdf, name="download_incident_brief_pdf"),

    # Forecast
    path("forecast/cyclone-monsoon", views.get_cyclone_monsoon_forecast, name="get_cyclone_monsoon_forecast"),
]
