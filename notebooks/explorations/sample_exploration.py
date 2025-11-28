# Databricks notebook source
source_schema = "source"
tables_df = spark.sql(f"SHOW TABLES IN {source_schema}")
tables = [row.tableName for row in tables_df.collect()]
display(tables)

# COMMAND ----------

table_list=spark.sql(f"SHOW TABLES IN stg").select("tableName")
for i in table_list.collect():
    print(i['tableName'])
    spark.sql(f"DROP TABLE IF EXISTS stg.{i['tableName']}")

# COMMAND ----------

tables_df = spark.sql(f"LIST '/Volumes/workspace/source/lake'")
tables = [row.name for row in tables_df.collect()]
display(tables)

# COMMAND ----------

tables_df = dbutils.fs.ls("/Volumes/workspace/source/lake/")
for i in tables_df:
    print(i.name)
    a=spark.read.csv(i.path)
    display(a)

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/source/lake/_schemas/etlfilmy")
    .load("/Volumes/workspace/source/lake/")
)
display(
    df,
    checkpointLocation="/Volumes/workspace/source/lake/_checkpoints/etlfilmy"
)
