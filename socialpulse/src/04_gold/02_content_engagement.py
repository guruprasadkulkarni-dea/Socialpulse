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
        drop_tables(content_engagement_table)
        delete_watermark_record(watermark_table,content_engagement_table)
    except Exception as e:
        raise f"Error in drop tables : {e}"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

res = get_df(content_engagement_table,clean_events_table,watermark_table)
clean_events_df = res[0]

# COMMAND ----------

if clean_events_df is not None:
    clean_events_df = clean_events_df.withColumn('updated_at', fn.to_timestamp(fn.col('updated_at')))
    window_spec = Window.partitionBy('content_type').orderBy(fn.col('engagement_score').desc())
    content_engagement_df = clean_events_df.groupBy('content_id', 'content_type') \
                            .agg(fn.coalesce(fn.sum(fn.when(((fn.col('event_type') == 'story_view') | (fn.col  ('event_type') == 'video_play') | (fn.col('event_type') == 'page_view')), 1).otherwise(0)),fn.lit(1)).alias('views_count'),fn.sum(fn.when(fn.col('event_type') == 'like', 1).otherwise(0)).alias('likes_count'),fn.sum(fn.when(fn.col('event_type') == 'share', 1).otherwise(0)).alias('shares_count'),fn.sum(fn.when(fn.col('event_type') == 'comment', 1).otherwise(0)).alias('comment_count')) \
                        .withColumn('engagement_score',fn.least(fn.coalesce(fn.round(fn.try_divide((fn.col('comment_count') + fn.col('shares_count') + fn.col('likes_count')), fn.col('views_count')),2),fn.lit(0)) * 100,fn.lit(100))) \
                        .withColumn('content_rank',fn.rank().over(window_spec))

display(content_engagement_df.limit(10))

# COMMAND ----------

if clean_events_df is not None:
  upsert_target_table(content_engagement_df,content_engagement_table,"target.content_id = source.content_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Updates watermark table for next run incremental run

# COMMAND ----------

current_timestamp = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata"))
df = spark.createDataFrame([[content_engagement_table,current_timestamp]],schema='entity_name string,watermark_value timestamp')
upsert_target_table(df,watermark_table,"target.entity_name = source.entity_name")

# COMMAND ----------

display(spark.read.table(watermark_table))