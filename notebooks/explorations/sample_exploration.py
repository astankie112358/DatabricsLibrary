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

# COMMAND ----------

from src.transformations.create_joins import join_config

def check_if_tables_join(table1, table2):
    for join in join_config[table1]['joins']:
        if join['other_table']==table2:
            return True
    return False 

def find_join_map(table_name, joins):
    left_to_join=joins
    matched = []
    for other_table in left_to_join:
        if check_if_tables_join(table_name, other_table):
            matched.append(other_table)
            left_to_join.remove(other_table)
    while len(left_to_join)>0:
        operation_made=False
        for other_table in left_to_join:
            for matched_table in matched:
                if check_if_tables_join(other_table, matched_table):
                    matched.append(other_table)
                    left_to_join.remove(other_table)
                    operation_made=True
        if not operation_made:
            return matched
    return matched
display(find_join_map('Rental_Items', ['Rentals', 'Movies','Copies']))
