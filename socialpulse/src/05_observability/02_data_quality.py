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
    drop_tables(data_quality_table)

# COMMAND ----------

pipeline_name_list = get_pipeline_table_names('dev',project_properties_table)

# COMMAND ----------

end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(days=2)
print(f"start_time {start_time}")
print(f"end_time {end_time}")

# COMMAND ----------

print(pipeline_name_list)
for pipeline in pipeline_name_list:
      print('pipeline----> ',pipeline)
      eligible_pl_dtls_df = spark.sql(f"""
                        with cte1 as(
                              select concat('event_log_',pl.pipeline_id) as mod_pipeline_name
                              ,pl.pipeline_id as pipeline_id
                              ,pl.name as pipeline_name
                              ,pl_upd.update_id as update_id
                              ,pl_upd.period_start_time as run_start_time
                              ,pl_upd.period_end_time as run_end_time
                              from system.lakeflow.pipelines pl
                              inner join system.lakeflow.pipeline_update_timeline pl_upd
                              on pl.pipeline_id = pl_upd.pipeline_id
                              where name = '{pipeline}' and pl_upd.period_start_time >= '{start_time}' and pl_upd.period_end_time <= '{end_time}'),
                        
                        cte2 as(
                              select cte1.pipeline_id
                              ,cte1.pipeline_name
                              ,cte1.update_id
                              ,cte1.run_start_time
                              ,cte1.run_end_time
                              ,concat(table_catalog,'.',table_schema,'.`',table_name,'`') as full_qualified_name
                              from system.information_schema.tables tab 
                              inner join cte1 on tab.table_name = cte1.mod_pipeline_name)
                        select * from cte2
                        """)
      
      if eligible_pl_dtls_df.take(1) == 0:
            print(f"No pipeline run found for {pipeline}")
            continue
      #display(eligible_pl_dtls_df)
      full_qualified_name = eligible_pl_dtls_df.select('full_qualified_name').dropDuplicates().collect()[0]['full_qualified_name']
      print(f"Full qualified pipeline id is {full_qualified_name}")

      full_qualified_name_df = spark.read.table(full_qualified_name) \
            .filter(((fn.col('event_type') == 'flow_progress') | (fn.col('event_type') == 'flow_definition')) & (fn.get_json_object(fn.to_json(fn.col('origin')), '$.pipeline_name') == str(pipeline))) \
            .select(fn.col('origin.request_id').alias('new_id'),fn.get_json_object(fn.col('details'), '$.flow_definition.output_dataset').alias('flow_name'))
      
      full_qualified_name_df = full_qualified_name_df.filter(fn.col('flow_name').isNotNull())
      #display(full_qualified_name_df)

      flow_name_df = eligible_pl_dtls_df.join(full_qualified_name_df,eligible_pl_dtls_df['update_id'] == full_qualified_name_df['new_id'])
      
      expect_dlt_df = spark.sql(f"""
            with cte3 as(
            select origin.request_id as id,
                  from_json(get_json_object(details, '$.flow_progress.data_quality.expectations'), 'Array<Struct<name string,dataset string,passed_records string,failed_records  string>>') as expectations_array,
                  get_json_object(details,'$.flow_progress.metrics.num_output_rows' ) as total_rows    
                  from {full_qualified_name}
                  where origin.pipeline_name = '{pipeline}'),

            cte4 as(
                  select id, explode(arrays_zip(expectations_array.name,expectations_array.passed_records,expectations_array.failed_records)) as exp_arr, total_rows from cte3),

            cte5 as(
                  select id, exp_arr.name as expectations_name, exp_arr.failed_records as failed_records, exp_arr.passed_records as passed_records, total_rows from 
                  cte4
            ),

            cte6 as(
            select *,round(cast(passed_records as int) * 1.0 / cast(total_rows as int),2) * 100 as pass_percentage from cte5 
            where expectations_name is not null)

            select * from cte6
            """)
      
      final_df = flow_name_df.join(expect_dlt_df, flow_name_df.new_id == expect_dlt_df.id) \
                              .select('pipeline_name','pipeline_id','flow_name','expectations_name','passed_records','failed_records','total_rows','pass_percentage','run_start_time','run_end_time','update_id').dropDuplicates(['pipeline_id','update_id','flow_name','expectations_name'])
      display(final_df)

      if final_df is not None:
            upsert_target_table(final_df,data_quality_table,"((target.pipeline_id = source.pipeline_id) and (target.update_id = source.update_id) and (target.flow_name = source.flow_name) and (target.expectations_name = source.expectations_name))")