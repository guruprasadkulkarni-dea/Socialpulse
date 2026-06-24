# Databricks notebook source
# MAGIC %run "../00_setup/common_functions"

# COMMAND ----------

# MAGIC %run "../00_setup/common_variables"

# COMMAND ----------

from pyspark.sql import functions as fn

# COMMAND ----------

dbutils.widgets.text("refersh_type","incremental")
p_refersh_type = dbutils.widgets.get("refersh_type")
print(p_refersh_type)

# COMMAND ----------

if p_refersh_type == 'full':
    drop_tables(pipeline_health_table)

# COMMAND ----------

pipeline_name_list = get_pipeline_table_names('dev',project_properties_table)
print(pipeline_name_list)

# COMMAND ----------

end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(days=1)

# COMMAND ----------

for table in pipeline_name_list:
       pipeline_health_df = spark.sql(f"""
                     select pl_upd.pipeline_id as pipeline_id,
                            pl.name as pipeline_name,
                            pl_upd.update_id as update_id,
                            pl_upd.result_state as state,
                            pl_upd.trigger_type as cause,
                            pl_upd.period_start_time as start_time,
                            pl_upd.period_end_time as end_time,
                            unix_timestamp(pl_upd.period_end_time) - unix_timestamp(pl_upd.period_start_time)  as duration_seconds
                     from system.lakeflow.pipelines as pl 
                     inner join system.lakeflow.pipeline_update_timeline as pl_upd
                     on pl.pipeline_id = pl_upd.pipeline_id
                     where pl_upd.period_end_time < '{end_time}' 
                     and pl_upd.period_start_time >= '{start_time}'
                     and pl.name = '{table}'
                     """)
       final_df = pipeline_health_df.dropDuplicates(["pipeline_id","update_id"])
       if pipeline_health_df is not None:
              upsert_target_table(final_df,pipeline_health_table,"((target.pipeline_id = source.pipeline_id) and (target.update_id = source.update_id))")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from socialpulse_catalog.config.obs_pipeline_health