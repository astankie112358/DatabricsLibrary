from pyspark.sql import SparkSession

def read_table(schema, table):
    return SparkSession.getActiveSession().table(f"{schema}.{table}")

def write_table(df, schema, table, mode="overwrite"):
    (
        df.write
          .format("delta")
          .mode(mode)
          .saveAsTable(f"{schema}.{table}")
    )