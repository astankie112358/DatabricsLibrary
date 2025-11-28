from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
from src.transformations.rename_columns import rename_columns, column_rename_map
from src.transformations.rename_tables import get_new_table_name
import dlt

catalog="workspace"
source_schema = "source"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

#####Load To Bronze#####################

def bronze_table_batch(file_name, table_name):
    @dlt.table(name=f"{catalog}.{bronze_schema}.{table_name}")
    def _table():
        return (
            spark.read.csv(f"/Volumes/{catalog}/{source_schema}/lake/{file_name}",header=True)
            .withColumn("loaded_time", current_timestamp())
        )
    return _table

def bronze_table_stream(path, table_name):
    @dlt.table(name=f"{catalog}.{bronze_schema}.{table_name}")
    def _table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load(path)
            .withColumn("loaded_time", current_timestamp())
        )

bronze_table_batch("ETLFILMY.csv", "ETLFILMY")
bronze_table_batch("ETLFIL_KRA.csv", "ETLFIL_KRA")
bronze_table_batch("ETLFIL_RODZ.csv", "ETLFIL_RODZ")
bronze_table_batch("ETLKLI.csv", "ETLKLI")
bronze_table_batch("ETLKRAJE.csv", "ETLKRAJE")
bronze_table_batch("ETLNOSNIKI.csv", "ETLNOSNIKI")
bronze_table_batch("ETLREZYSERZY.csv", "ETLREZYSERZY")
bronze_table_batch("ETLRODZAJE.csv", "ETLRODZAJE")
bronze_table_stream("/Volumes/workspace/source/orders/", "ETLWYPO")
bronze_table_batch("ETLWYPO_NOS.csv", "ETLWYPO_NOS")

#####Load To Silver#####################

def silver_table(table_name):
    new_name = get_new_table_name(table_name)
    @dlt.table(name=f"{catalog}.{silver_schema}.{new_name}")
    def _table():
        df = dlt.read(f"{catalog}.{bronze_schema}.{table_name}")
        df = add_loaded_time(df)
        df = rename_columns(df, column_rename_map[f"{table_name}"])
        return df
    
silver_table("ETLFILMY")
silver_table("ETLFIL_KRA")
silver_table("ETLFIL_RODZ")
silver_table("ETLKLI")
silver_table("ETLKRAJE")
silver_table("ETLNOSNIKI")
silver_table("ETLREZYSERZY")
silver_table("ETLRODZAJE")
silver_table("ETLWYPO")
silver_table("ETLWYPO_NOS")

