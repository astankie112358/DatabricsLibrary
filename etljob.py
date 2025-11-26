from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
import dlt

def read_table(schema, table):
    return SparkSession.getActiveSession().table(f"{schema}.{table}")

def write_table(df, schema, table, mode="overwrite"):
    (
        df.write
          .format("delta")
          .mode(mode)
          .saveAsTable(f"{schema}.{table}")
    )

source_schema = "source"
tables_df = dbutils.fs.ls("/Volumes/workspace/source/lake/")
tables = [row.name for row in tables_df]

print("Starting ETL pipeline...")

for table in tables:
    print(f"\n---- Processing table: {table} ----")
    
    @dlt.table(name=f"stg_{table}")
    def create_table(table_name=table):
        df=spark.read.format("csv").option("header", "true").load(f"/Volumes/workspace/source/lake/{table}")
        return df

print("\nETL pipeline completed successfully.")
