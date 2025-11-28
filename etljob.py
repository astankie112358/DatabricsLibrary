from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
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
            spark.read.csv(f"/Volumes/{catalog}/{source_schema}/lake/{file_name}")
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
bronze_table_stream("/Volumes/workspace/source/lake/", "ETLWYPO")
bronze_table_batch("ETLWYPO_NOS.csv", "ETLWYPO_NOS")

#####Load To Silver#####################

@dlt.table(name=f"{catalog}.{silver_schema}.ETLFILMY")
def silver_ETLFILMY():
    df = dlt.read(f"{catalog}.{bronze_schema}.ETLFILMY")
    df = add_loaded_time(df)
    return df

