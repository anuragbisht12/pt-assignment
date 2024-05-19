from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from data_quality.custom_sensor import DataQualitySensor
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "advertiseX_monitoring_pipeline",
    default_args=default_args,
    description="Monitoring and alerting pipeline for AdvertiseX",
    schedule_interval="*/15 * * * *",  # Runs every 15 minutes
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id="start",
    )

    # Data quality checks
    impressions_quality_check = DataQualitySensor(
        task_id="impressions_quality_check",
        table_path="s3://test-bucket/processed/impressions",
    )

    clicks_quality_check = DataQualitySensor(
        task_id="clicks_quality_check",
        table_path="s3://test-bucket/processed/clicks",
    )

    bid_requests_quality_check = DataQualitySensor(
        task_id="bid_requests_quality_check",
        table_path="s3://test-bucket/processed/bid_requests",
    )

    enriched_quality_check = DataQualitySensor(
        task_id="enriched_quality_check",
        table_path="s3://test-bucket/processed/enriched",
    )

    # Task to send email alert if any data quality check fails
    email_alert = EmailOperator(
        task_id="email_alert",
        to="test@domain.com",
        subject="Data Quality Issue Detected",
        html_content="<p>One or more data quality checks have failed.</p>",
    )

    end = DummyOperator(
        task_id="end",
    )

    start >> [impressions_quality_check, clicks_quality_check, bid_requests_quality_check, enriched_quality_check] >> email_alert >> end
