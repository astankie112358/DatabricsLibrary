from pyspark.sql.functions import current_timestamp

def add_loaded_time(df):
    return df.withColumn("loaded_time", current_timestamp())