# Databricks notebook source
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import zoneinfo

# COMMAND ----------

dbutils.widgets.text("refersh_type","incremental")
p_refersh_type = dbutils.widgets.get("refersh_type")
print(p_refersh_type)

# COMMAND ----------

# MAGIC %run "../00_setup/common_variables"

# COMMAND ----------

# MAGIC %run "../00_setup/common_functions"

# COMMAND ----------

if p_refersh_type.lower() == 'full':
    try:
        drop_tables(hourly_active_users_table)
        delete_watermark_record(watermark_table,hourly_active_users_table)
    except Exception as e:
        raise f"Error in drop tables : {e}"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

res = get_df(hourly_active_users_table,clean_events_table,watermark_table)
clean_events_df = res[0]

# COMMAND ----------

display(clean_events_df.limit(10))

# COMMAND ----------

if clean_events_df is not None:
    hourly_active_users_df = clean_events_df.groupBy(fn.date_format(fn.col('timestamp'),'yyyy-MM-dd').alias('date'),fn.lpad(fn.date_format(fn.col('timestamp'), 'HH'), 2, '0') \
        .alias('hour'),fn.col('platform'),fn.col('country')) \
        .agg(fn.countDistinct('user_id').alias('unique_users'),fn.count(fn.col('event_id')).alias('total_events'),fn.countDistinct('session_id').alias('new_sessions'))

display(hourly_active_users_df.limit(10))

# COMMAND ----------

if clean_events_df is not None:
  upsert_target_table(hourly_active_users_df,hourly_active_users_table,"(target.date = source.date) and (target.hour = source.hour) and (target.platform = source.platform) and (target.country = source.country)")

# COMMAND ----------

current_timestamp = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata"))
df = spark.createDataFrame([[hourly_active_users_table,current_timestamp]],schema='entity_name string,watermark_value timestamp')
upsert_target_table(df,watermark_table,"target.entity_name = source.entity_name")

# COMMAND ----------

display(spark.sql(f'select * from {watermark_table}'))