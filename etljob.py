from pyspark.sql.functions import current_timestamp
from src.transformations.add_loaded_time import add_loaded_time
import dlt

catalog="workspace"
source_schema = "source"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

#####Load To Bronze#####################

def bronze_table(file_name, table_name):
    @dlt.table(name=f"{catalog}.{bronze_schema}.{table_name}")
    def _table():
        return (
            spark.read.csv(f"/Volumes/{catalog}/{source_schema}/lake/{file_name}")
            .withColumn("loaded_time", current_timestamp())
        )
    return _table

bronze_table("ETLFILMY.csv", "ETLFILMY")
bronze_table("ETLFIL_KRA.csv", "ETLFIL_KRA")
bronze_table("ETLFIL_RODZ.csv", "ETLFIL_RODZ")
bronze_table("ETLKLI.csv", "ETLKLI")
bronze_table("ETLKRAJE.csv", "ETLKRAJE")
bronze_table("ETLNOSNIKI.csv", "ETLNOSNIKI")
bronze_table("ETLREZYSERZY.csv", "ETLREZYSERZY")
bronze_table("ETLRODZAJE.csv", "ETLRODZAJE")
bronze_table("ETLWYPO.csv", "ETLWYPO")
bronze_table("ETLWYPO_NOS.csv", "ETLWYPO_NOS")

#####Load To Silver#####################

@dlt.table(name=f"{catalog}.{silver_schema}.ETLFILMY")
def silver_ETLFILMY():
    df = dlt.read(f"{catalog}.{bronze_schema}.ETLFILMY")
    df = add_loaded_time(df)
    return df

# tables = [row.name for row in tables_df]
# for table in tables:
#     csv_name=table
#     table_name=table.replace(".csv","")
#     @dlt.create_table(name=f"{catalog}.{bronze_schema}.{table_name}")
#     def create_table(table_name=table):
#         df=spark.read.format("csv").option("header", "true").load(f"/Volumes/workspace/source/lake/{csv_name}")
#         return df

# for table in tables:
#     table.replace(".csv","")
#     table=table.replace(".csv","")  
#     @dlt.create_table(name=f"{catalog}.{silver_schema}.{table}")
#     def create_table(table_name=table):
#         df=spark.sql(f"SELECT * FROM {bronze_schema}.b_{table}")
#         return df

