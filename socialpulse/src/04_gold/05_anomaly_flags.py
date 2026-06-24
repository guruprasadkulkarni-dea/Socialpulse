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

res = get_df(anomaly_flags_table,clean_events_table,watermark_table)

clean_events_df = res[0]
watermark_value = res[1]

print('watermark_value-----> ',watermark_value)

if watermark_value != '':
    silver_event_hist_df        = spark.read.table(clean_event_table)
    active_user_df              = clean_events_df.select('user_id').distinct()
    suspicious_activity_df      = silver_event_hist_df.join(active_user_df, silver_event_hist_df['user_id'] == active_user_df['user_id'], how='left_semi').filter(silver_event_hist_df['updated_at'] > watermark_value)

# COMMAND ----------

number_of_event_cond             = "fn.col('number_of_events') > 40"
#login_country_and_timestamp_cond = "(fn.col('prev_country') != fn.col('country')) & ((fn.to_timestamp(fn.col('prev_timestamp'), 'yyyy-MM-dd HH:mm').cast('long') - fn.to_timestamp(fn.col('timestamp_mod'), 'yyyy-MM-dd HH:mm').cast('long')) < 1800) & (fn.col('prev_country').isNotNull())"
login_country_and_timestamp_cond = "(fn.col('prev_country') != fn.col('country')) & ((fn.to_timestamp(fn.col('prev_timestamp')).cast('long') - fn.to_timestamp(fn.col('event_timestamp')).cast('long')) < 1800) & (fn.col('prev_country').isNotNull())"
ip_address_count_cond            = "fn.size(fn.collect_set('ip_address').over(window_spec2)) > 3"
spike_cond                       = "fn.col('spike') == 1"
suspicious_activity_cond         = "fn.col('suspicious_activity') == 1"
bot_attack_cond                  = "fn.col('bot_attack') == 1"

# COMMAND ----------

case_severity_0 = "fn.col('case_severity') == 0"
case_severity_1 = "fn.col('case_severity') == 1"
case_severity_2 = "fn.col('case_severity') == 2"
case_severity_3 = "fn.col('case_severity') == 3"
case_severity_4 = "fn.col('case_severity') == 4"
case_severity_5 = "fn.col('case_severity') == 5"
case_severity_6 = "fn.col('case_severity') == 6"
case_severity_7 = "fn.col('case_severity') == 7"

# COMMAND ----------

issue_severity_0 = 'normal'
issue_severity_1 = 'spike'
issue_severity_2 = 'suspicious'
issue_severity_3 = 'bot'
issue_severity_4 = issue_severity_1 + '_' + issue_severity_2
issue_severity_5 = issue_severity_1 + '_' + issue_severity_3
issue_severity_6 = issue_severity_2 + '_' + issue_severity_3
issue_severity_7 = issue_severity_1 + '_' + issue_severity_2 + '_' + issue_severity_3

# COMMAND ----------

flag_reason_sev0 = 'normal'
flag_reason_sev1 = 'unusual network traffic observed'
flag_reason_sev2 = 'multiple different geo location login detected'
flag_reason_sev3 = 'multiple IP with same user-id'
flag_reason_sev4 = flag_reason_sev1 + ';' + flag_reason_sev2
flag_reason_sev5 = flag_reason_sev1 + ';' + flag_reason_sev3
flag_reason_sev6 = flag_reason_sev2 + ';' + flag_reason_sev3
flag_reason_sev7 = flag_reason_sev1 + ';' + flag_reason_sev2 + '_' + flag_reason_sev3

# COMMAND ----------

if clean_events_df is not None:
    window_spec1 = Window.partitionBy('user_id','event_timestamp').orderBy(fn.col('event_timestamp'))
    window_spec2 = Window.partitionBy('user_id').orderBy(fn.col('event_timestamp'))
    input_timestamp_format ="yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    output_timestamp_without_seconds_and_timezone = "yyyy-MM-dd HH:mm"
    output_timestamp_without_timezone = "yyyy-MM-dd HH:mm:ss"

    mod_df = clean_events_df.withColumn('event_timestamp',fn.date_format(fn.to_timestamp(fn.col('timestamp'),input_timestamp_format),output_timestamp_without_seconds_and_timezone)) \
                            .withColumn('number_of_events',fn.size(fn.collect_set(fn.col('event_id')).over(window_spec1))) \
                            .withColumn('spike',fn.when(eval(number_of_event_cond),1).otherwise(0)) \
                            .withColumn('bot_attack',fn.when(eval(ip_address_count_cond),1).otherwise(0)) \
                            .withColumn('ip_address_list',fn.collect_set('ip_address').over(window_spec1))

    print('watermark_value----> ',watermark_value)
    display(mod_df.limit(10))               
    if (watermark_value is None) or (watermark_value == '') :
        print('111111111111')
        mod_df = mod_df.withColumn('prev_country',fn.lag('country',1).over(window_spec2)) \
                       .withColumn('prev_timestamp',fn.lag('event_timestamp',1).over(window_spec2)) \
                       .withColumn('suspicious_activity',fn.when(eval(login_country_and_timestamp_cond),1) \
                                                           .otherwise(0))
    
    else:
        print('22222222222222222')
        mod_df_cols = mod_df.columns
        #print('mod_df_cols---->', mod_df_cols)
        suspicious_activity_df = suspicious_activity_df.unionByName(clean_events_df) \
                       .withColumn('event_timestamp',fn.date_format(fn.to_timestamp(fn.col('timestamp'),input_timestamp_format),output_timestamp_without_seconds_and_timezone)) \
                       .withColumn('prev_country',fn.lag('country',1).over(window_spec2)) \
                       .withColumn('prev_timestamp',fn.lag('event_timestamp',1).over(window_spec2)) \
                       .withColumn('suspicious_activity',fn.when(eval(login_country_and_timestamp_cond),1) \
                                                           .otherwise(0))
        suspicious_activity_df = suspicious_activity_df \
                       .select('user_id','event_timestamp','suspicious_activity') \
                       .dropDuplicates()

        mod_df = mod_df.join(suspicious_activity_df,['user_id','event_timestamp']) \
                       .select(*mod_df_cols,suspicious_activity_df['suspicious_activity'])
    
    mod_df = mod_df.filter(eval(suspicious_activity_cond) | eval(bot_attack_cond) | eval(spike_cond))
    display(mod_df.limit(10))

# COMMAND ----------

anomaly_flags_df = mod_df.withColumn('case_severity', fn.when(eval(spike_cond) \
                                & eval(suspicious_activity_cond) & eval(bot_attack_cond),7) \
                                .when(eval(suspicious_activity_cond) & eval(bot_attack_cond),6) \
                                .when(eval(spike_cond) & eval(bot_attack_cond),5) \
                                .when(eval(spike_cond) & eval(suspicious_activity_cond),4) \
                                .when(eval(bot_attack_cond),3) \
                                .when(eval(suspicious_activity_cond),2) \
                                .when(eval(spike_cond),1)
                                .otherwise(0)) \
        .withColumn('flag_type',fn.when(eval(case_severity_0),issue_severity_0) \
                                    .when(eval(case_severity_1),issue_severity_1) \
                                    .when(eval(case_severity_2),issue_severity_2) \
                                    .when(eval(case_severity_3),issue_severity_3) \
                                    .when(eval(case_severity_4),issue_severity_4) \
                                    .when(eval(case_severity_5),issue_severity_5) \
                                    .when(eval(case_severity_6),issue_severity_6) \
                                    .otherwise(issue_severity_7)) \
        .withColumn('flag_reason',fn.when(eval(case_severity_0),flag_reason_sev0) \
                                    .when(eval(case_severity_1),flag_reason_sev1) \
                                    .when(eval(case_severity_2),flag_reason_sev2) \
                                    .when(eval(case_severity_3),flag_reason_sev3) \
                                    .when(eval(case_severity_4),flag_reason_sev4) \
                                    .when(eval(case_severity_5),flag_reason_sev5) \
                                    .when(eval(case_severity_6),flag_reason_sev6) \
                                    .otherwise(flag_reason_sev7)) \
        .withColumn('severity',fn.when(eval(case_severity_0),'NA') \
                                    .when(eval(case_severity_1),'low') \
                                    .when(eval(case_severity_2),'low') \
                                    .when(eval(case_severity_3),'medium') \
                                    .when(eval(case_severity_4),'medium') \
                                    .when(eval(case_severity_5),'high') \
                                    .when(eval(case_severity_6),'high') \
                                    .otherwise('high')) \
        .withColumn('first_seen',fn.date_format(fn.to_timestamp(fn.first('timestamp').over(window_spec1),input_timestamp_format),output_timestamp_without_timezone)) \
        .withColumn('last_seen',fn.date_format(fn.to_timestamp(fn.last('timestamp').over(window_spec1),input_timestamp_format),output_timestamp_without_timezone)) \
        .withColumn('detection_timestamp',fn.date_format(fn.to_timestamp(fn.current_timestamp(),input_timestamp_format),output_timestamp_without_timezone)) \
        .select(fn.explode(mod_df['ip_address_list']).alias('ip_address'),'user_id',fn.col('event_timestamp'),'case_severity','flag_type','flag_reason','number_of_events','first_seen','last_seen','severity','detection_timestamp') \
        .dropDuplicates()
    
display(anomaly_flags_df)

# COMMAND ----------

if clean_events_df is not None:
  upsert_target_table(anomaly_flags_df,anomaly_flags_table,"(target.user_id = source.user_id) and (target.event_timestamp == source.event_timestamp)")

# COMMAND ----------

current_timestamp = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata"))
df = spark.createDataFrame([[anomaly_flags_table,current_timestamp]],schema='entity_name string,watermark_value timestamp')
upsert_target_table(df,watermark_table,"target.entity_name = source.entity_name")

# COMMAND ----------

display(spark.read.table(watermark_table))