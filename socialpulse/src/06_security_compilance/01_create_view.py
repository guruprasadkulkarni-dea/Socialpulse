# Databricks notebook source
# MAGIC %run "../00_setup/common_variables"

# COMMAND ----------

# MAGIC %run "../00_setup/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_pipeline_name","streaming") 
p_pipeline_name = dbutils.widgets.get("p_pipeline_name")
print('p_pipeline_name----->',p_pipeline_name)

# COMMAND ----------

if p_pipeline_name == 'batch':
    pii_columns = fetch_pii_columns('users')
    print('user_pii_columns----->',pii_columns)
    table_name = clean_users_table
    view_name = clean_users_view
else:
    pii_columns = fetch_pii_columns('events')
    print('event_pii_columns----->',pii_columns)
    table_name = clean_events_table
    view_name = clean_events_view

# COMMAND ----------

table_column_list = spark.read.table(table_name).schema.names
print(table_column_list)
select_col_str = ''

for column in table_column_list:
    if column in pii_columns:
        select_col_str += f'socialpulse_catalog.security.data_mask({column}) as {column},'
    else:
        select_col_str += f'{column},'
select_col_str = select_col_str[:-1]
print('select_col_str------> ', select_col_str)

# COMMAND ----------

create_data_masking_function()
create_row_filters_function()

# COMMAND ----------

create_view(table_name,select_col_str,view_name,'country')

# COMMAND ----------


display(spark.sql(f"select * from {view_name}").limit(10))