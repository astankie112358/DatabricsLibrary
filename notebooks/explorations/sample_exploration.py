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

# COMMAND ----------

from src.transformations.rename_columns import column_rename_map

def rename_columns(df, mapping):
    for old_col, new_col in mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

df = spark.read.table("workspace.bronze.etlfilmy")
df = rename_columns(df, column_rename_map["ETLFILMY"])
display(df)

# COMMAND ----------

from src.transformations.create_joins import return_joins_for_table

join_cfg=return_joins_for_table("Rentals")
if join_cfg and len(join_cfg)>0:
    for join in join_cfg:
        display(join['other_table'])
        #right_df = spark.read(f"workspace.silver".{join['other_table']}")
