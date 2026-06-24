# Databricks notebook source
import requests
import json
import time

# COMMAND ----------

# MAGIC %run "../00_setup/common_functions"

# COMMAND ----------

# MAGIC %run "../00_setup/common_variables"

# COMMAND ----------

dbutils.widgets.text("p_pipeline_name","") 
p_pipeline_name = dbutils.widgets.get("p_pipeline_name")
print('p_pipeline_name----->',p_pipeline_name)

# COMMAND ----------

dbutils.widgets.text("p_refresh_type", "") 
p_refresh_type = dbutils.widgets.get("p_refresh_type")
print('p_refresh_type----->',p_refresh_type)

# COMMAND ----------

if p_refresh_type.lower().strip() == 'full':
    try:
        if p_pipeline_name.lower() == 'streaming':
            drop_tables(clean_events_table)
        else:
            drop_tables(clean_ads_staged_table)
            drop_tables(clean_ads_table)
            drop_tables(clean_users_staged_table)
            drop_tables(clean_users_table)
    except Exception as e:
        print(e)

# COMMAND ----------

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

response = requests.get(
    f"https://{host}/api/2.0/pipelines",
    headers={"Authorization": f"Bearer {token}"},
    params={"filter": f"name LIKE '{p_pipeline_name}'"}
)

pipelines = response.json().get("statuses", [])
pipeline_id = pipelines[0]["pipeline_id"] if pipelines else None
print("pipeline_id-->", pipeline_id)

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host  = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

if p_refresh_type.strip() == "full":
    print('full load is executing')
    payload = {"full_refresh": True}
else:
    print('incremental load is executing')
    payload = {"full_refresh": False}

response = requests.post(
    f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
    headers=headers,
    json=payload
)
print(response.json())

# COMMAND ----------


update_id = response.json()["update_id"]

while True:
    status_resp = requests.get(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
        headers=headers
    ).json()
    
    state = status_resp["update"]["state"]
    print(f"Pipeline state: {state}")
    
    if state in ["COMPLETED"]:
        break
    elif state in ["FAILED", "CANCELED"]:
        raise Exception(f"Pipeline update failed with state: {state}")
    
    time.sleep(15)