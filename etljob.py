from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
from src.transformations.rename_columns import rename_columns
from src.transformations.rename_tables import get_new_table_name
from src.transformations.create_joins import return_joins_for_table
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
            .option("cloudFiles.partitionColumns", "") 
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

def silver_table_stream(table_name):
    new_name = get_new_table_name(table_name)
    @dlt.table(name=f"{catalog}.{silver_schema}.{new_name}")
    def _table():
        df = spark.readStream.table(f"{catalog}.{bronze_schema}.{table_name}")
        df = add_loaded_time(df)
        df = rename_columns(df, table_name)
        return df

silver_table_stream("ETLFILMY")
silver_table_stream("ETLFIL_KRA")
silver_table_stream("ETLFIL_RODZ")
silver_table_stream("ETLKLI")
silver_table_stream("ETLKRAJE")
silver_table_stream("ETLNOSNIKI")
silver_table_stream("ETLREZYSERZY")
silver_table_stream("ETLRODZAJE")
silver_table_stream("ETLWYPO")
silver_table_stream("ETLWYPO_NOS")

def gold_fact_table(table_name):
    new_name = "f_orders"
    @dlt.table(name=f"{catalog}.{gold_schema}.{new_name}")
    def _table():
        df = spark.readStream.table(f"{catalog}.{silver_schema}.{table_name}")
        df2 = spark.readStream.table(f"{catalog}.{silver_schema}.rental_items")
        df = df.join(
            df2,
            df2.rental_id == df.rental_id
        )
        df = df.drop(df2.rental_id, df2.loaded_time)
        return df
def gold_dim_table(table_name):
    new_name = "d_movies"
    @dlt.table(name=f"{catalog}.{gold_schema}.{new_name}")
    def _table():
        df = spark.readStream.table(f"{catalog}.{silver_schema}.{table_name}")
        df2 = spark.readStream.table(f"{catalog}.{silver_schema}.movie_genre")
        df3 = spark.readStream.table(f"{catalog}.{silver_schema}.movie_country")
        df4 = spark.readStream.table(f"{catalog}.{silver_schema}.genres")
        df5 =
        df = df.join(
            df2
        return df

gold_fact_table("rentals")
        

