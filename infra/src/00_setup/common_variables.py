# Databricks notebook source
catalog_name = dbutils.secrets.get(scope="socialpulse", key="catalog_name")
config_schema = 'config'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
security_schema = 'security'
storage_account = dbutils.secrets.get(scope="socialpulse", key="storage-account")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Config Tables

# COMMAND ----------

watermark_table = f'{catalog_name}.{config_schema}.watermark'
project_properties_table = f'{catalog_name}.{config_schema}.project_properties'
pipeline_health_table = f'{catalog_name}.{security_schema}.obs_pipeline_health'
pipeline_cost_table = f'{catalog_name}.{security_schema}.obs_pipeline_cost'
table_growth_table = f'{catalog_name}.{security_schema}.obs_table_growth'
data_quality_table = f'{catalog_name}.{security_schema}.obs_data_quality'
rule_config_table = f'{catalog_name}.{security_schema}.rule_config'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Silver Layer Staged Tables

# COMMAND ----------

clean_ads_staged_table       = f'{catalog_name}.{silver_schema}.clean_ads_staged'
clean_users_staged_table     = f'{catalog_name}.{silver_schema}.clean_users_staged'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Silver Layer Tables

# COMMAND ----------

clean_ads_table       = f'{catalog_name}.{silver_schema}.clean_ads'
clean_events_table     = f'{catalog_name}.{silver_schema}.clean_events'
clean_users_table     = f'{catalog_name}.{silver_schema}.clean_users'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Gold Layer Tables

# COMMAND ----------

user_session_summary_table      = f'{catalog_name}.{gold_schema}.user_session_summary'
content_engagement_table        = f'{catalog_name}.{gold_schema}.content_engagement'
ad_performance_table            = f'{catalog_name}.{gold_schema}.ad_performance'
hourly_active_users_table       = f'{catalog_name}.{gold_schema}.hourly_active_users'
anomaly_flags_table             = f'{catalog_name}.{gold_schema}.anomaly_flags'

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View Name

# COMMAND ----------

clean_events_view     = f'{catalog_name}.{security_schema}.vw_clean_events'
clean_users_view     = f'{catalog_name}.{security_schema}.vw_clean_users'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### landing path

# COMMAND ----------

landing_path = f"abfss://landing@{storage_account}.dfs.core.windows.net"
landing_path_events = f"{landing_path}/events/batch_*/*.json"
landing_path_users = f"{landing_path}/users/*.csv"
landing_path_ads = f"{landing_path}/ads/*.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Schema path

# COMMAND ----------

schema_path =f'abfss://config@{storage_account}.dfs.core.windows.net'
schema_path_events =f'{schema_path}/schemas/events'
schema_path_users =f'{schema_path}/schemas/users'
schema_path_ads =f'{schema_path}/schemas/ads'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Misc

# COMMAND ----------

appid = dbutils.secrets.get(scope="socialpulse", key="appid")
