# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as fn
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

catalog_name = spark.conf.get("catalog_name", "socialpulse_catalog")

# COMMAND ----------

df = spark.read.table(f'{catalog_name}.config.project_properties')
schema_path = df.filter((fn.col('field_key') == 'schema_location_path') & (fn.col('entity') == 'ads') & (fn.col('env') == 'dev')  & (fn.col('quality') == 'bronze')) .select('field_value').collect()[0]['field_value']
landing_path = df.filter((fn.col('field_key') == 'landing_path') & (fn.col('env') == 'dev') & (fn.col('entity') == 'ads')  & (fn.col('quality') == 'bronze')).select('field_value').collect()[0]['field_value']

print('schema_path : ',schema_path)
print('landing_path : ',landing_path)


# COMMAND ----------

@dp.table(
    name    = f'{catalog_name}.bronze.raw_ads',
    comment = 'bronze table for ads',
    table_properties = {'quality': 'bronze',
                        'delta.feature.timestampNtz': 'supported'}
)

def raw_ads():
    return (
            spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format', 'csv')
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("header","true")
            .load(landing_path)
            .withColumn('ingestion_timestamp', fn.current_timestamp().cast("timestamp_ntz"))
            .withColumn('update_timestamp', fn.current_timestamp().cast("timestamp_ntz")) 
            .withColumn('source_file_name', fn.col('_metadata.file_name'))
            .withColumn('source_file_path', fn.col('_metadata.file_path')))