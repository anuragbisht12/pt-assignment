"""This module contains custom sensor created for measuring data quality."""
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class DataQualitySensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, table_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_path = table_path

    def poke(self, context):
        spark = SparkSession.builder \
            .appName("AirflowDataQualityCheck") \
            .getOrCreate()

        # Load data from Delta Lake table
        df = spark.read.format("delta").load(self.table_path)
        
        # Example check: ensure the table is not empty
        if df.count() == 0:
            self.log.error(f"Data quality check failed: {self.table_path} is empty")
            return False

        # Example check: ensure no null values in critical columns
        if df.filter(col("timestamp").isNull() | col("user_id").isNull()).count() > 0:
            self.log.error(f"Data quality check failed: {self.table_path} contains null values in critical columns")
            return False

        return True
