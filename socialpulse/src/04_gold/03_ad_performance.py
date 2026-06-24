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
        drop_tables(ad_performance_table)
        delete_watermark_record(watermark_table,ad_performance_table)
    except Exception as e:
        raise f"Error in drop tables : {e}"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

res1 = get_df(ad_performance_table,clean_ads_table,watermark_table)
clean_ads_df = res1[0]

res2 = get_df(ad_performance_table,clean_events_table,watermark_table)
clean_events_df = res2[0]

# COMMAND ----------

if (clean_ads_df is not None) and (clean_events_df is not None):
    mod_df1 = clean_events_df.filter(fn.col('ad_id').isNotNull()).groupBy('ad_id',) \
                            .agg(fn.sum(fn.when((fn.col('event_type') == 'ad_click') ,1) .otherwise(0)).alias('ad_clicks_count'), \
                            fn.sum(fn.when((fn.col('event_type') == 'ad_impression') ,1) .otherwise(0)).alias('ad_impressions_count')) \
                            .withColumn('ctr',
                            fn.round(fn.try_divide(fn.col('ad_clicks_count'), fn.col('ad_impressions_count')),2))
    display(mod_df1.limit(10))
    mod_df2 = clean_ads_df.filter(fn.col('ad_id').isNotNull()) \
                        .select('ad_id', 'campaign_id', fn.round(fn.coalesce(fn.try_divide(fn.col('spend_usd'), fn.col('budget_usd')) * 100, \
                        fn.lit(0.00)),2).alias('spend_percentage'))
    ad_performance_df = mod_df1.join(mod_df2, mod_df1['ad_id'] == mod_df2['ad_id']) \
                                .select(mod_df1.ad_id, mod_df2.campaign_id,mod_df1.ad_clicks_count, mod_df1.ad_impressions_count,mod_df1.ctr,mod_df2.spend_percentage)
    display(ad_performance_df.limit(10))


# COMMAND ----------

if (clean_ads_df is not None) and (clean_events_df is not None):
  upsert_target_table(ad_performance_df,ad_performance_table,"target.ad_id = source.ad_id")

# COMMAND ----------

current_timestamp = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata"))
df = spark.createDataFrame([[ad_performance_table,current_timestamp]],schema='entity_name string,watermark_value timestamp')
upsert_target_table(df,watermark_table,"target.entity_name = source.entity_name")

# COMMAND ----------

display(spark.sql(f"select * from {watermark_table}"))