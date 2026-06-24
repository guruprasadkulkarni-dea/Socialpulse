# Databricks notebook source
from datetime import datetime, timedelta
from pyspark.sql import functions as fn
from delta.tables import DeltaTable

# COMMAND ----------

def get_df(entity_name,silver_layer_table,watermark_table):
    watermark_value = ''
    if not spark.catalog.tableExists(f'{watermark_table}'):
        print(f'{watermark_table} doesnt exist')
        entity_raw_df = spark.read.table(f'{silver_layer_table}')
    else:
        watermark_value = spark.read.table(f'{watermark_table}').filter(fn.col('entity_name') == f'{entity_name}').collect()
        if len(watermark_value) == 0 or watermark_value[0]['watermark_value'] is None:
            print(f'No watermark value found for {entity_name}')
            entity_raw_df = spark.read.table(f'{silver_layer_table}')
            watermark_value = ''
        else:
            watermark_value = watermark_value[0]['watermark_value'] - timedelta(minutes=30)
            entity_raw_df = spark.read.table(f'{silver_layer_table}').filter(fn.col('updated_at') > watermark_value)
            print('watermark_value ---> ', watermark_value)
    return [entity_raw_df,watermark_value]

# COMMAND ----------

def upsert_target_table(source_df,target_table,condition):
  if not spark.catalog.tableExists(f'{target_table}'):
    source_df.write.format('delta').mode('overwrite').saveAsTable(f'{target_table}')
  else:
      target_delta = DeltaTable.forName(spark, f'{target_table}')
      target_delta.alias("target") \
          .merge(source=source_df.alias("source"), condition=condition) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()

# COMMAND ----------

def get_pipeline_table_names(env,project_properties_table):
    input_df = spark.read.table(f'{project_properties_table}').filter((fn.col('env') == 'dev') & (fn.col('quality') == 'bronze') & (fn.col('field_key') == 'pipeline') & ~(fn.col('field_key').like('New%'))) \
        .toPandas()
    pipeline_name_list = input_df['field_value'].drop_duplicates()
    return pipeline_name_list

# COMMAND ----------

def drop_tables(table_name):
    if spark.catalog.tableExists(table_name):
        try:
            print(f'Deleting the table {table_name} due to full refresh ')
            spark.sql(f'drop table {table_name}')
            print(f'Table {table_name} deleted succesfully')
        except Exception as e:
            raise e
    else:
        print(f'Table {table_name} doesnt exist')

# COMMAND ----------

def delete_watermark_record(watermark_table,entity_name):
    if spark.catalog.tableExists(f'{watermark_table}'):
        spark.sql(f"""
                  delete from {watermark_table} where entity_name = '{entity_name}'
                  """)
        print(f'Watermark record deleted for {entity_name}')
    else:
        print(f'Table {watermark_table} doesnt exist')

# COMMAND ----------

def fetch_pii_columns(entity_name):
    pii_columns_df = spark.sql(f"""
                            select field_value from socialpulse_catalog.config.project_properties where entity = '{entity_name}' and field_key = 'pii_columns'""")
    
    pii_columns = pii_columns_df.collect()[0]['field_value'].split(',')
    return pii_columns

# COMMAND ----------

def create_data_masking_function():
    spark.sql("""
            create or replace function socialpulse_catalog.security.data_mask(column_name STRING)
            return 
                case
                    when column_name = 'email' then
                        case
                            when is_account_group_member('socialpulse_admin') then column_name
                            when is_account_group_member('socialpulse_analyst_UK') or is_account_group_member('socialpulse_analyst_US') then concat(left(column_name,1),'***',substring(column_name,locate('@',column_name)))
                            else '*****@****.***'
                        end
                    else
                        case
                            when is_account_group_member('socialpulse_admin') then column_name
                            when ((is_account_group_member('socialpulse_analyst_UK')) or ( is_account_group_member('socialpulse_analyst_US'))) then concat(left(column_name,1),'***')
                            else '*****'
                        end
                end""")

# COMMAND ----------

def create_row_filters_function():
    spark.sql("""
    create or replace function socialpulse_catalog.security.row_filters(column_name string)
    return 
        case 
            when is_account_group_member('socialpulse_admin') then true
            when (
                ((is_account_group_member('socialpulse_analyst_UK') and column_name = 'UK') or
                 (is_account_group_member('socialpulse_analyst_US') and column_name = 'US'))
                and
                (is_account_group_member('socialpulse_analyst_all_tiers') or
                 is_account_group_member('socialpulse_analyst_free_tiers') or
                 is_account_group_member('socialpulse_analyst_premium_tiers') or
                 is_account_group_member('socialpulse_analyst_business_tiers'))
            ) then true
            else false
        end
    """)

# COMMAND ----------

def create_view(table_name,select_col_str,view_name,row_filter_col):
    spark.sql(f"""
              create or replace view {view_name} as 
              select {select_col_str} from {table_name} 
              where socialpulse_catalog.security.row_filters('{row_filter_col}') 
              """)