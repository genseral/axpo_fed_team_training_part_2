# Databricks notebook source
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("target_bronze", "")
dbutils.widgets.text("target_silver", "")
dbutils.widgets.text("target_gold", "")
dbutils.widgets.text("source_schema", "")

# COMMAND ----------

catalog_name  = dbutils.widgets.get("catalog_name")
target_bronze = dbutils.widgets.get("target_bronze")
source_schema = dbutils.widgets.get("source_schema")

x = spark.sql(f""" 
  WITH T1 AS (
    SELECT 
    table_catalog
  , table_schema
  , table_name
  , '{target_bronze}' as target_schema
  FROM {catalog_name}.information_schema.tables
  WHERE table_name like 'bronze_%' and table_schema = '{source_schema}'
  )
  SELECT 
    'CREATE OR REPLACE VIEW ' ||
      table_catalog || '.' || 
      target_schema || '.' || 
      table_name || ' AS ' || 
      'SELECT * FROM ' || 
      table_catalog || '.' || 
      table_schema || '.' || 
      table_name || ';' AS sql_script
  FROM T1 
""").collect()
for i in x:
  spark.sql(i.sql_script)

# COMMAND ----------

catalog_name  = dbutils.widgets.get("catalog_name")
target_silver = dbutils.widgets.get("target_silver")
source_schema = dbutils.widgets.get("source_schema")

x = spark.sql(f""" 
  WITH T1 AS (
    SELECT 
    table_catalog
  , table_schema
  , table_name
  , '{target_silver}' as target_schema
  FROM {catalog_name}.information_schema.tables
  WHERE table_name like 'silver_%' and table_schema = '{source_schema}'
  )
  SELECT 
    'CREATE OR REPLACE VIEW ' ||
      table_catalog || '.' || 
      target_schema || '.' || 
      table_name || ' AS ' || 
      'SELECT * FROM ' || 
      table_catalog || '.' || 
      table_schema || '.' || 
      table_name || ';' AS sql_script
  FROM T1 
""").collect()
for i in x:
  spark.sql(i.sql_script)

# COMMAND ----------

catalog_name  = dbutils.widgets.get("catalog_name")
target_gold = dbutils.widgets.get("target_gold")
source_schema = dbutils.widgets.get("source_schema")

x = spark.sql(f""" 
  WITH T1 AS (
    SELECT 
    table_catalog
  , table_schema
  , table_name
  , '{target_gold}' as target_schema
  FROM {catalog_name}.information_schema.tables
  WHERE table_name like 'gold_%' and table_schema = '{source_schema}'
  )
  SELECT 
    'CREATE OR REPLACE VIEW ' ||
      table_catalog || '.' || 
      target_schema || '.' || 
      table_name || ' AS ' || 
      'SELECT * FROM ' || 
      table_catalog || '.' || 
      table_schema || '.' || 
      table_name || ';' AS sql_script
  FROM T1 
""").collect()
for i in x:
  spark.sql(i.sql_script)
