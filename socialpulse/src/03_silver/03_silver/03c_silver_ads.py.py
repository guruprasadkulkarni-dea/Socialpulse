# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as fn

# COMMAND ----------

def get_rules(entity_name, action_type):
    """
    Reads data quality conditions from the config table.
    action_type can be: 'warn' (expect_all), 'drop' (expect_all_or_drop), 'fail' (expect_all_or_fail)
    """
    rules_df = (
        spark.read.table("socialpulse_catalog.config.rule_config")
        .filter((fn.col("entity") == entity_name) & (fn.col("action_type") == action_type))
        .collect()
    )
    return {row["field_name"]: row["rule_expression"] for row in rules_df}

# COMMAND ----------

def get_select_expr(entity,quality):
    """
    Build SelectExpr Dynamically
    """
    raw_fields = spark.read \
                        .table("socialpulse_catalog.config.project_properties") \
                        .filter((fn.col('field_key') == 'schema') & \
                        (fn.col('entity') == entity) & (fn.col('env') == 'dev') \
                        & (fn.col('quality') == quality)).select('field_value').collect()[0][0]
    field_list = []
    for rec in raw_fields.split(','):
        field_name = rec.split(':')[0].strip()
        field_data_type = rec.split(':')[1].strip()
        if field_data_type.lower() == 'string':
            field_list.append(fn.col(field_name))        
        else:
            field_list.append(fn.expr(f"TRY_CAST({field_name} AS {field_data_type})").alias(field_name))
    return field_list

# COMMAND ----------

warn_rules = get_rules("ads", "warn")
drop_rules = get_rules("ads", "drop")

# COMMAND ----------

select_expr = get_select_expr("ads", "silver")

# COMMAND ----------

@dp.table(
    name = "socialpulse_catalog.silver.clean_ads_staged",
    comment="table for Clean silver table records",
        table_properties = {'quality': 'silver',
                        'delta.feature.timestampNtz': 'supported',
                        'pipelines.reset.allowed': 'true',
                        'delta.enableDeletionVectors' : 'true'}
)
@dp.expect_all(warn_rules)              
@dp.expect_all_or_drop(drop_rules)      
def clean_ads_staged():
    return (
        spark.readStream \
        .option("skipChangeCommits", "true") \
        .table("socialpulse_catalog.bronze.raw_ads") \
        .filter(fn.col("_rescued_data").isNull()) \
        .select(*select_expr)
    )

# COMMAND ----------

dp.create_streaming_table(
    name = "socialpulse_catalog.silver.clean_ads",
    comment="table for Clean silver table records",
    table_properties = {"quality": "silver",
                        "delta.feature.timestampNtz": "supported"})

dp.create_auto_cdc_flow(
    target             = "socialpulse_catalog.silver.clean_ads",
    source             = "socialpulse_catalog.silver.clean_ads_staged",  # ← staged!
    keys               = ["ad_id"],
    sequence_by        = "updated_at",
    stored_as_scd_type = 2
)