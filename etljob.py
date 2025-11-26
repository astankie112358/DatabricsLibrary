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
target_schema = "stg"
tables_df = spark.sql(f"SHOW TABLES IN {source_schema}")
tables = [row.tableName for row in tables_df.collect()]

print("Starting ETL pipeline...")

for table in tables:
    print(f"\n---- Processing table: {table} ----")
    
    @dlt.table(name=f"stg_{table}")
    def create_table(table_name=table):
        df = read_table(source_schema, table_name)
        print(f"Loaded {source_schema}.{table_name}  (rows: {df.count()})")
        df = add_loaded_time(df)
        print(f"Processed {table_name}")
        return df

print("\nETL pipeline completed successfully.")
