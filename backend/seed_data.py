"""
Seed Script: Ingests 100% genuine real-time live meteorological telemetry
from 38+ Indian stations across all states into the Django database.
"""
import os
import sys
from pathlib import Path

# Ensure backend directory is in python path
_backend_dir = Path(__file__).resolve().parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "weathernexus.settings")

import django
django.setup()

from weather_api.services.live_ingestion import fetch_and_ingest_live_weather

def seed_database():
    print("Connecting to live national meteorological stream across 38+ Indian cities...")
    res = fetch_and_ingest_live_weather(wipe_old=True)
    print("Django Database live seeding completed successfully!")
    print(f"-> Ingested: {res['ingested_count']} 100% Real Live Observations")
    print(f"-> Sources: {', '.join(res['sources'])}")
    print(f"-> Synced At: {res['synced_at']}")

if __name__ == "__main__":
    seed_database()
