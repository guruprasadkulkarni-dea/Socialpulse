# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

"""if p_refersh_type.lower() == 'full':
    df = spark.read.table('system.information_schema.tables').filter(fn.col('table_name') == 'raw_events').select(fn.concat(fn.col('table_catalog'),fn.lit('.'),fn.col('table_schema'),fn.lit('.`'),fn.col('table_name')).alias('full_qualified_name'))
    full_qualified_name = df.select('full_qualified_name').collect()[0]['full_qualified_name']
    if spark.catalog.tableExists(full_qualified_name):
        try:
            print('Deleting the table due to full refresh ---> ',full_qualified_name)
            spark.sql(f'drop table {full_qualified_name}')
        except Exception as e:
            print(f'Table Deletion failed due to {e}')"""

# COMMAND ----------

df = spark.read.table('socialpulse_catalog.config.project_properties')
#schema_path = df.filter((fn.col('field_key') == 'schema_location_path') & (fn.col('entity') == 'events') & (fn.col('env') == 'dev')  & (fn.col('quality') == 'bronze')) .select('field_value').collect()[0]['field_value']

raw_schema_str = df.filter((fn.col('field_key') == 'schema') & (fn.col('entity') == 'events') & (fn.col('env') == 'dev')  & (fn.col('quality') == 'bronze')) .select('field_value').collect()[0]['field_value']

raw_schema_str = raw_schema_str.replace(':',' ')
event_schema = StructType.fromDDL(raw_schema_str)

landing_path = df.filter((fn.col('field_key') == 'landing_path') & (fn.col('env') == 'dev') & (fn.col('entity') == 'events')  & (fn.col('quality') == 'bronze')).select('field_value').collect()[0]['field_value']

print('schema : ',event_schema)
print('landing_path : ',landing_path)

# COMMAND ----------

@dp.table(
    name    = 'socialpulse_catalog.bronze.raw_events',
    comment = 'bronze table for events',
    schema  = event_schema,
    table_properties = {'quality': 'bronze',
                        'delta.feature.timestampNtz': 'supported'}
)

def raw_events():
    return (
            spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format', 'json')
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(f'{landing_path}')
            .withColumn('ingestion_timestamp', fn.current_timestamp().cast("timestamp_ntz"))
            .withColumn('update_timestamp', fn.current_timestamp().cast("timestamp_ntz")) 
            .withColumn('source_file_name', fn.col('_metadata.file_name'))
            .withColumn('source_file_path', fn.col('_metadata.file_path')))