# Databricks notebook source
def assign_permission(permission,catalog_name,schema_name,appid):
    spark.sql(f""" Grant use catalog on catalog `{catalog_name}` to `{appid}`""")
    spark.sql(f""" Grant use schema on schema `{catalog_name}`.`{schema_name}` to `{appid}`""")
    if (permission == 'execute'):
        spark.sql(f""" Grant {permission} on schema `{catalog_name}`.`{schema_name}` to `{appid}`""")
    else:
        spark.sql(f""" Grant {permission} on schema `{catalog_name}`.`{schema_name}` to `{appid}`""")

# COMMAND ----------

appid = '34bdd15a-8bf9-4dac-a1b1-51f7d10c3a6c'
assign_permission('select','system','lakeflow',appid)
assign_permission('execute','socialpulse_catalog','security',appid)
assign_permission('ALL PRIVILEGES','socialpulse_catalog','gold',appid)
assign_permission('ALL PRIVILEGES','socialpulse_catalog','silver',appid)
assign_permission('ALL PRIVILEGES','socialpulse_catalog','config',appid)
assign_permission('manage','socialpulse_catalog','gold',appid)
assign_permission('manage','socialpulse_catalog','config',appid)
assign_permission('manage','socialpulse_catalog','silver',appid)
assign_permission('manage','socialpulse_catalog','security',appid)
assign_permission('ALL PRIVILEGES','socialpulse_catalog','security',appid)