from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
import dlt

catalog="workspace"
source_schema = "source"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
tables_df = dbutils.fs.ls("/Volumes/workspace/source/lake/")
tables = [row.name for row in tables_df]
for table in tables:
    csv_name=table
    table_name=table.replace(".csv","")
    @dlt.create_table(name=f"{catalog}.{bronze_schema}.{table_name}")
    def create_table(table_name=table):
        df=spark.read.format("csv").option("header", "true").load(f"/Volumes/workspace/source/lake/{csv_name}")
        return df

for table in tables:
    table.replace(".csv","")
    table=table.replace(".csv","")  
    @dlt.create_table(name=f"{catalog}.{silver_schema}.{table}")
    def create_table(table_name=table):
        df=spark.sql(f"SELECT * FROM {bronze_schema}.b_{table}")
        return df

