from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "advertiseX_data_pipeline",
    default_args=default_args,
    description="Data pipeline for AdvertiseX",
    schedule_interval=None,  # Schedule interval set to None for manual/triggered runs
    start_date=days_ago(1),
) as dag:

    start = DummyOperator(
        task_id="start",
    )

    start_spark_job = BashOperator(
        task_id="start_spark_job",
        bash_command="spark-submit --class com.advertiseX.DataProcessing /path/to/spark-job.jar",
    )

    start_spark_job = BashOperator(
        task_id="start_spark_job",
        bash_command="""
        aws s3 cp s3://test-bucket/scripts/etl.py /tmp/etl.py && \
        spark-submit /tmp/etl.py
        """,
    )

    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="advertiseX_data_pipeline",  # The same DAG ID to trigger itself
        wait_for_completion=False,
    )

    end = DummyOperator(
        task_id="end",
    )

    start >> start_spark_job >> end >> trigger_next_run
