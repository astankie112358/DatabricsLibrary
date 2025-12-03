from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
from src.transformations.rename_columns import rename_columns
from src.transformations.rename_tables import get_new_table_name
from src.transformations.create_joins import find_join_map
from src.transformations.data_fix import replace_polish_characters
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
            spark.read.csv(f"/Volumes/{catalog}/{source_schema}/lake/{file_name}",header=True, encoding="windows-1250")
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
        df = spark.read.table(f"{catalog}.{bronze_schema}.{table_name}")
        df = add_loaded_time(df)
        df = replace_polish_characters(df)
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

#####Load To Gold#####################

def gold_table(table_name, new_table_name, join_tables=None, drop_columns=None):
    @dlt.table(name=f"{catalog}.{gold_schema}.{new_table_name}")
    def _table():
        df_main = (
            spark.readStream
            .option("skipChangeCommits", "true")
            .table(f"{catalog}.{silver_schema}.{table_name}")
        )
        joins={}
        if join_tables is not None:
            joins.update({table_name: spark.read.table(f"{catalog}.{silver_schema}.{table_name}") for table_name in join_tables})
            df_main=find_join_map(df_main, table_name, joins, drop_columns=None)
        return df_main

#Configure the denormalization

table_name="copies"
join_tables=["directors", "movie_country", "movie_genre","genres","countries","movies"]
drop_columns=["director_id","country_id","genre_id"]     

gold_table(table_name, "d_movies", join_tables, drop_columns)

table_name="clients"
join_tables=None
drop_columns=None   
gold_table(table_name, "d_customers", join_tables, drop_columns)

table_name="rental_items"
join_tables=["rentals"]
drop_columns=None
gold_table(table_name, "f_orders", join_tables, drop_columns)


