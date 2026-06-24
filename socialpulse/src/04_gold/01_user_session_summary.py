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
        drop_tables(user_session_summary_table)
        delete_watermark_record(watermark_table,user_session_summary_table)
    except Exception as e:
        raise f"Error in drop tables : {e}"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

res = get_df(user_session_summary_table,clean_events_table,watermark_table)
clean_events_df = res[0]

# COMMAND ----------

if clean_events_df is not None:
    window_spec1 = Window.partitionBy('user_id','platform')
    window_spec2 = Window.partitionBy('user_id').orderBy(fn.desc('platform_used_count'))

    session_duration_df = clean_events_df.groupBy('user_id','session_id').agg((fn.max('timestamp').cast("long") - fn.min('timestamp').cast("long")).alias('session_duration'))
    total_session_duration_df = session_duration_df.groupBy('user_id').agg(fn.sum('session_duration').alias('sum_session_duration'),fn.count('session_id').alias('total_session_per_user'))
    avg_session_duration_df = total_session_duration_df.select('user_id', 'sum_session_duration',(fn.col('sum_session_duration') / fn.col('total_session_per_user')).cast('decimal(10,2)').alias('avg_session_duration_in_seconds'))

    last_seen_timestamp_df = clean_events_df.groupBy('user_id').agg(fn.max('timestamp').alias('last_seen_timestamp_in_IST'))

    count_of_platform_df = clean_events_df.withColumn('platform_used_count', fn.count('session_id').over(window_spec1)) \
                                .dropDuplicates(['user_id','session_id','platform'])

    most_used_platform_df = count_of_platform_df.withColumn('rank1', fn.row_number().over(window_spec2)).filter(fn.col('rank1') == 1) \
                   .select('user_id',fn.col('platform').alias('most_used_platform')) 

    total_events_per_user_df = clean_events_df.groupBy('user_id').agg(fn.count('session_id').alias('total_events_per_user'))

    total_df    = total_session_duration_df.alias("t")
    avg_df      = avg_session_duration_df.alias("a")
    platform_df = most_used_platform_df.alias("p")
    last_seen_df = last_seen_timestamp_df.alias("ls")
    events_df   = total_events_per_user_df.alias("e")

    #user_session_summary_df = platform_df \
    #    .join(total_df, platform_df['user_id'] == total_df['user_id']) \
    #    .join(last_seen_df, platform_df['user_id'] == last_seen_df['user_id']) \
    #    .join(avg_df, platform_df['user_id'] == avg_df['user_id']) \
    #    .join(events_df, platform_df['user_id'] == events_df['user_id']) \
    #    .select(platform_df['user_id'],avg_df['total_session_per_user'] \
    #    ,fn.round(avg_df['avg_session_duration_in_seconds'],2).alias##('avg_session_duration_in_seconds'),platform_df['most_used_platform'] \
    #    ,last_seen_df['last_seen_timestamp_in_IST'],events_df['total_events_per_user']) \
    #    .dropDuplicates()

    user_session_summary_df = platform_df \
        .join(total_df, 'user_id') \
        .join(last_seen_df, 'user_id') \
        .join(avg_df, 'user_id') \
        .join(events_df, 'user_id') \
        .select(platform_df['user_id'],total_df['total_session_per_user'] \
        ,fn.round(avg_df['avg_session_duration_in_seconds'],2).alias('avg_session_duration_in_seconds'),platform_df['most_used_platform'] \
        ,last_seen_df['last_seen_timestamp_in_IST'],events_df['total_events_per_user']) \
        .dropDuplicates()

display(user_session_summary_df.limit(10))

# COMMAND ----------

if clean_events_df is not None:
  upsert_target_table(user_session_summary_df,user_session_summary_table,"target.user_id = source.user_id")

# COMMAND ----------

current_timestamp = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata"))
df = spark.createDataFrame([[user_session_summary_table,current_timestamp]],schema='entity_name string,watermark_value timestamp')
upsert_target_table(df,watermark_table,"target.entity_name = source.entity_name")

# COMMAND ----------

display(spark.read.table(watermark_table))

# COMMAND ----------

print(total_session_duration_df.columns)
print(avg_session_duration_df.columns)
print(most_used_platform_df.columns)
print(last_seen_timestamp_df.columns)
print(total_events_per_user_df.columns)