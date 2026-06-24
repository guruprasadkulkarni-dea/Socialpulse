-- Databricks notebook source
create external location if not exists claudesocialpulsedl_ext_dl_landing
url 'abfss://landing@claudesocialpulsedl.dfs.core.windows.net/'
with (storage credential `claudescocialpulse-storage-credential`)
comment 'external location for storage account claudesocialpulsedl for container landing'

-- COMMAND ----------

create external location if not exists claudesocialpulsedl_ext_dl_bronze
url 'abfss://bronze@claudesocialpulsedl.dfs.core.windows.net/'
with (storage credential `claudescocialpulse-storage-credential`)
comment 'external location for storage account claudesocialpulsedl for container bronze'

-- COMMAND ----------

create external location if not exists claudesocialpulsedl_ext_dl_silver
url 'abfss://silver@claudesocialpulsedl.dfs.core.windows.net/'
with (storage credential  `claudescocialpulse-storage-credential`)
comment 'external location for storage account claudesocialpulsedl for container silver'

-- COMMAND ----------

create external location if not exists claudesocialpulsedl_ext_dl_gold
url 'abfss://gold@claudesocialpulsedl.dfs.core.windows.net/'
with (storage credential  `claudescocialpulse-storage-credential`)
comment 'external location for storage account claudesocialpulsedl for container gold'

-- COMMAND ----------

create external location if not exists claudesocialpulsedl_ext_dl_config
url 'abfss://config@claudesocialpulsedl.dfs.core.windows.net/'
with (storage credential `claudescocialpulse-storage-credential`)
comment 'external location for storage account claudesocialpulsedl for container config'

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://landing@claudesocialpulsedl.dfs.core.windows.net/'

-- COMMAND ----------

show external locations

-- COMMAND ----------

create catalog if not exists socialpulse_catalog 
managed location 'abfss://config@claudesocialpulsedl.dfs.core.windows.net/';
create schema if not exists socialpulse_catalog.bronze;
create schema if not exists socialpulse_catalog.silver;
create schema if not exists socialpulse_catalog.gold;
create schema if not exists socialpulse_catalog.config;
create schema if not exists socialpulse_catalog.security;

-- COMMAND ----------

drop table if exists socialpulse_catalog.config.project_properties;

-- COMMAND ----------

create table if not exists socialpulse_catalog.config.project_properties
(
  env string,
  entity string,
  quality string,
  field_key string,
  field_value string,
  load_timestamp timestamp default current_timestamp(),
  is_active boolean default true
)
comment 'This is a table which stores the miscellenous variable information'
tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add Bronze Landing Path

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','events','bronze','landing_path','abfss://landing@claudesocialpulsedl.dfs.core.windows.net/events/*/*.json',true),
('dev','users','bronze','landing_path','abfss://landing@claudesocialpulsedl.dfs.core.windows.net/users/*.csv',true),
('dev','ads','bronze','landing_path','abfss://landing@claudesocialpulsedl.dfs.core.windows.net/ads/*.csv',true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add Bronze Schema Location Path

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','events','bronze','schema_location_path','abfss://config@claudesocialpulsedl.dfs.core.windows.net/schemas/events',true),
('dev','users','bronze','schema_location_path','abfss://config@claudesocialpulsedl.dfs.core.windows.net/schemas/users',true),
('dev','ads','bronze','schema_location_path','abfss://config@claudesocialpulsedl.dfs.core.windows.net/schemas/ads',true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add Bronze Schema

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','events','bronze','schema','event_id:string, user_id:string,session_id:string, event_type:string,content_id:string, content_type:string,ad_id:string, platform:string,device_type:string, country:string,city:string, user_agent:string,ip_address:string,timestamp:string, duration_ms:string,scroll_depth:string, updated_at:string,batch_id:string,source_file_name:string,source_file_path:string,ingestion_timestamp:timestamp_ntz,update_timestamp:timestamp_ntz,_rescued_data:string',true),
('dev','users','bronze','schema','user_id:string, username:string,email:string,full_name:string,age:string, gender:string,country:string, city:string,platform:string, tier:string,followers:string, following:string,posts_count:string, is_active:string,signup_date:string, updated_at:string,source_file_name:string,source_file_path:string,ingestion_timestamp:timestamp_ntz,update_timestamp:timestamp_ntz,_rescued_data:string',true),
('dev','ads','bronze','schema','ad_id:string, campaign_id:string,ad_name:string, ad_type:string,target_country:string, target_tier:string,is_active:string, start_date:string,end_date:string, cpc:string,cpm:string, budget_usd:string,spend_usd:string, updated_at:string,source_file_name:string,source_file_path:string,ingestion_timestamp:timestamp_ntz,update_timestamp:timestamp_ntz,_rescued_data:string',true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add Silver Schema

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','events','silver','schema','event_id:string, user_id:string,session_id:string, event_type:string,content_id:string, content_type:string,ad_id:string, platform:string,device_type:string, country:string,city:string, user_agent:string,ip_address:string,timestamp:timestamp, duration_ms:int,scroll_depth:double, updated_at:timestamp,batch_id:string,source_file_name:string,source_file_path:string,ingestion_timestamp:timestamp_ntz,update_timestamp:timestamp_ntz,_rescued_data:string',true),
('dev','users','silver','schema','user_id:string, username:string,email:string,full_name:string,age:string, gender:string,country:string, city:string,platform:string, tier:string,followers:string, following:string,posts_count:string, is_active:string,signup_date:date, updated_at:timestamp,source_file_name:string,source_file_path:string,ingestion_timestamp:timestamp_ntz,update_timestamp:timestamp_ntz,_rescued_data:string',true),
('dev','ads','silver','schema','ad_id:string, campaign_id:string,ad_name:string, ad_type:string,target_country:string, target_tier:string,is_active:string, start_date:date,end_date:date, cpc:double,cpm:double, budget_usd:double,spend_usd:double,updated_at:timestamp,source_file_name:string,source_file_path:string,ingestion_timestamp:timestamp_ntz,update_timestamp:timestamp_ntz,_rescued_data:string',true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add bronze pipeline names

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','ads','bronze','pipeline','batch',true),
('dev','events','bronze','pipeline','streaming',true),
('dev','users','bronze','pipeline','batch',true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Silver Pipeline Dependent Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### PII Columns

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','events','bronze','pii_columns','user_id,ip_address,user_agent',true),
('dev','users','bronze','pii_columns','user_id, username, email, full_name',true)

-- COMMAND ----------

CREATE TABLE IF not EXISTS socialpulse_catalog.config.rule_config (
    rule_id          STRING,     -- UUID
    entity           STRING,     -- users/events/ads
    field_name       STRING,     -- age, email etc
    rule_expression  STRING,     -- age BETWEEN 18 AND 100
    action_type      STRING,     -- warn/drop/fail
    is_active        BOOLEAN,    
    effective_from   DATE,       
    effective_to     DATE,       
    reprocess_scope  STRING      -- full/from_effective_date/none
)
comment 'This is a table which stores the DLT Expect rules'
tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add expect rules for users

-- COMMAND ----------

INSERT INTO socialpulse_catalog.config.rule_config
SELECT uuid(), 'users', 'user_id', 'user_id IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'users', 'updated_at', 'updated_at IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'users', 'is_active', 'is_active IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'users', 'tier', 'tier IS NOT NULL', 'warn', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'users', 'signup_date', 'signup_date IS NOT NULL', 'warn', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'users', 'country', 'country IS NOT NULL', 'warn', true, current_date(), null, 'N'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add expect rules for events

-- COMMAND ----------

INSERT INTO socialpulse_catalog.config.rule_config
SELECT uuid(), 'events', 'event_id', 'event_id IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'events', 'updated_at', 'updated_at IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'events', 'timestamp', 'timestamp IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'events', 'user_id', 'user_id IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'events', 'event_type', 'event_type IS NOT NULL', 'warn', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'events', 'country', 'country IS NOT NULL', 'warn', true, current_date(), null, 'N'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Add expect rules for ads

-- COMMAND ----------

INSERT INTO socialpulse_catalog.config.rule_config
SELECT uuid(), 'ads', 'ad_id', 'ad_id IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'ads', 'updated_at', 'updated_at IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'ads', 'start_date', 'start_date IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'ads', 'is_active', 'is_active IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'ads', 'end_date', 'is_active = true OR end_date IS NOT NULL', 'drop', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'ads', 'target_country', 'target_country IS NOT NULL', 'warn', true, current_date(), null, 'N' UNION ALL
SELECT uuid(), 'ads', 'target_tier', 'target_tier IS NOT NULL', 'warn', true, current_date(), null, 'N'

-- COMMAND ----------

insert into socialpulse_catalog.config.project_properties(env,entity,quality,field_key,field_value,is_active)
values
('dev','ads','bronze','batch_pipeline_id','b9194383-8faa-4523-a77b-60229d74a40b',true),
('dev','events','bronze','streaming_pipeline_id','6c8c20c7-e53c-4b14-9cf4-cbce4ae7fc94',true),
('dev','users','bronze','batch_pipeline_id','b9194383-8faa-4523-a77b-60229d74a40b',true)

-- COMMAND ----------


select * from socialpulse_catalog.config.project_properties

-- COMMAND ----------

select * from socialpulse_catalog.config.rule_config