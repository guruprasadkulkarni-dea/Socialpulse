# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as fn
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

"""is_full_refresh = spark.conf.get("pipelines.isFullRefresh", "false") == "true"
print('is_full_refresh-------> ',is_full_refresh)
if is_full_refresh:
    df = spark.read.table('system.information_schema.tables').filter(fn.col('table_name') == 'raw_users').select(fn.concat(fn.col('table_catalog'),fn.lit('.'),fn.col('table_schema'),fn.lit('.`'),fn.col('table_name')).alias('full_qualified_name'))
    full_qualified_name = df.select('full_qualified_name').collect()[0]['full_qualified_name']
    if spark.catalog.tableExists(full_qualified_name):
        try:
            print('Deleting the table due to full refresh ---> ',full_qualified_name)
            spark.sql(f'drop table {full_qualified_name}')
        except Exception as e:
            print(f'Table Deletion failed due to {e}')"""

# COMMAND ----------

df = spark.read.table('socialpulse_catalog.config.project_properties')
schema_path = df.filter((fn.col('field_key') == 'schema_location_path') & (fn.col('entity') == 'users') & (fn.col('env') == 'dev')  & (fn.col('quality') == 'bronze')) .select('field_value').collect()[0]['field_value']
landing_path = df.filter((fn.col('field_key') == 'landing_path') & (fn.col('env') == 'dev') & (fn.col('entity') == 'users')  & (fn.col('quality') == 'bronze')).select('field_value').collect()[0]['field_value']
print('schema_path : ',schema_path)
print('landing_path : ',landing_path)

# COMMAND ----------

@dp.table(
    name    = 'socialpulse_catalog.bronze.raw_users',
    comment = 'bronze table for users',
    table_properties = {'quality': 'bronze',
                        'delta.feature.timestampNtz': 'supported'}
)

def raw_users():
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
