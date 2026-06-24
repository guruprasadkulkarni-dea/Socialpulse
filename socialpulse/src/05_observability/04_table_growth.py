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
    drop_tables(table_growth_table)

# COMMAND ----------

pipeline_name_list = get_pipeline_table_names('dev',project_properties_table)
print(pipeline_name_list)

# COMMAND ----------

for pipeline in pipeline_name_list:
      pipeline_id_df = spark.sql(f"""
            select pipeline_id
            from  system.lakeflow.pipelines 
            where name = '{pipeline}'""")
      table_name = f'event_log_{pipeline_id_df.collect()[0]["pipeline_id"]}'

      table_details_df = spark.sql(f"""
            select concat(table_catalog , '.', table_schema, '.', '`',table_name,'`') as full_qualified_name
            from system.information_schema.tables
            where table_name = '{table_name}'""")
      full_qualified_name = table_details_df.collect()[0]['full_qualified_name']
      print(full_qualified_name)

      table_growth_detail_df = spark.sql(f""" describe detail {full_qualified_name}""") \
                        .select(fn.split(fn.col('name'), "\\.").getItem(0).alias('table_catalog')
                                ,fn.split(fn.col('name'), "\\.").getItem(1).alias('table_schema')
                                ,fn.split(fn.col('name'), "\\.").getItem(2).alias('table_name')
                                ,fn.col('numFiles').alias('num_files')
                                ,fn.col('sizeInBytes').alias('size_bytes')
                                ,fn.round((fn.col('sizeInBytes') * 1.0 / (1024 * 1024)),2).alias('size_mb')
                                ,fn.col('lastmodified').alias('last_modified')
                                ,fn.col('createdAt').alias('snapshot_date')
                              )
      table_growth_formatted_df = spark.sql(f"""describe formatted {full_qualified_name}""") \
                                    .filter((fn.col('col_name') == 'Statistics') | (fn.col('col_name') == 'Catalog') | (fn.col('col_name') == 'Database') | (fn.col('col_name') == 'Table')) \
                                    .groupBy() \
                                    .pivot("col_name", ["Catalog", "Database", "Table", "Statistics"]) \
                                    .agg(fn.first("data_type")) \
                                    .select('Catalog', 'Database', 'Table',(fn.split(fn.trim(fn.split(fn.col('Statistics'),'\\,').getItem(1)),r' ').getItem(0)).cast('int').alias('num_rows'))
      
      table_growth_final_df = table_growth_detail_df.alias('df1').join(table_growth_formatted_df.alias('df2'), fn.expr("(df1.table_catalog == df2.Catalog) and (df1.table_schema == df2.Database) and (df1.table_name == df2.Table)")).select('df1.table_catalog','df1.table_schema','df1.table_name','df2.num_rows','num_files','size_bytes','size_mb','last_modified','snapshot_date')

      if table_growth_final_df is not None:
            upsert_target_table(table_growth_final_df,table_growth_table,"(target.table_name == source.table_name) and (target.snapshot_date == source.snapshot_date)")
      display(table_growth_final_df)