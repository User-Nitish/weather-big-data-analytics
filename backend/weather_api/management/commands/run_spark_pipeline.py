from django.core.management.base import BaseCommand
from weather_api.services.spark_pipeline import run_spark_big_data_job


class Command(BaseCommand):
    help = "Executes the Apache Spark Big Data meteorological analytics pipeline."

    def handle(self, *args, **options):
        self.stdout.write(self.style.NOTICE("Initializing Apache Spark 4.3.0 Big Data Engine..."))
        result = run_spark_big_data_job()
        self.stdout.write(self.style.SUCCESS("Apache Spark Big Data pipeline executed successfully!"))
        self.stdout.write(f"-> Engine: {result.get('engine')}")
        self.stdout.write(f"-> Status: {result.get('status')}")
        self.stdout.write(f"-> Records Processed: {result.get('records_processed')}")
        self.stdout.write(f"-> Runtime: {result.get('runtime_seconds')} seconds")
        self.stdout.write(f"-> Throughput: {result.get('throughput_records_sec')} records/sec")
        self.stdout.write(f"-> Top Risk Climate Zone: {result.get('climate_zones', [{}])[0].get('climate_zone', 'N/A')}")
        self.stdout.write(f"-> Anomalies Detected: {len(result.get('detected_anomalies', []))}")
