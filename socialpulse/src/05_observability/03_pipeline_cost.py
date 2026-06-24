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
    drop_tables(pipeline_cost_table)

# COMMAND ----------

pipeline_name_list = get_pipeline_table_names('dev',project_properties_table)
print(pipeline_name_list)

# COMMAND ----------

end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(days=1)

# COMMAND ----------

for table in pipeline_name_list:
      pipeline_cost_df = spark.sql(f"""
            select pl.pipeline_id as pipeline_id
                  ,pl.name as pipeline_name
                  ,pl_upd.update_id
                  ,billing.sku_name
                  ,billing.usage_date
                  ,billing.usage_start_time as usage_start_time
                  ,billing.usage_end_time as usage_end_time
                  ,billing.usage_quantity as dbu_consumed
                  ,round(billing.usage_quantity * lp.pricing.default,2) as estimated_cost_usd
                  ,billing.workspace_id  
            from system.billing.usage as billing
            inner join  system.lakeflow.pipelines as pl on billing.usage_metadata.dlt_pipeline_id = pl.pipeline_id
            inner join system.billing.list_prices as lp on lp.sku_name = billing.sku_name 
            inner join system.lakeflow.pipeline_update_timeline as pl_upd on pl.pipeline_id = pl_upd.pipeline_id
            where billing_origin_product = 'DLT' and
                  billing.usage_start_time >= '{start_time}' and
                  billing.usage_end_time < '{end_time}' and
                  pl.name = '{table}'
            """).dropDuplicates()
           
      if pipeline_cost_df is not None:
            display(pipeline_cost_df)
            upsert_target_table(pipeline_cost_df,pipeline_cost_table,"((target.update_id = source.update_id) and (target.dbu_consumed = source.dbu_consumed) and (target.estimated_cost_usd = source.estimated_cost_usd))")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.lakeflow.pipeline_update_timeline