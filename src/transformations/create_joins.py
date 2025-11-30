import src.errors.custom_errors as e
from pyspark.sql import functions as F

join_config = {
    "Movies": {
        "Directors": {"on": "director_id", "how": "left", "prefix": "dir_"},
        "Movie_Country": {"on": "movie_id", "how": "left", "prefix": "mc_"},
        "Movie_Genre": {"on": "movie_id", "how": "left", "prefix": "mg_"},
        "Copies": {"on": "movie_id", "how": "left", "prefix": "copy_"}
    },

    "Directors": {
        "Movies": {"on": "director_id", "how": "left", "prefix": "movie_"}
    },

    "Countries": {
        "Movie_Country": {"on": "country_id", "how": "left", "prefix": "mc_"}
    },

    "Movie_Country": {
        "Movies": {"on": "movie_id", "how": "left", "prefix": "movie_"},
        "Countries": {"on": "country_id", "how": "left", "prefix": "country_"}
    },

    "Genres": {
        "Movie_Genre": {"on": "genre_id", "how": "left", "prefix": "mg_"}
    },

    "Movie_Genre": {
        "Movies": {"on": "movie_id", "how": "left", "prefix": "movie_"},
        "Genres": {"on": "genre_id", "how": "left", "prefix": "genre_"}
    },

    "Clients": {
        "Rentals": {"on": "client_id", "how": "left", "prefix": "rent_"}
    },

    "Rentals": {
        "Clients": {"on": "client_id", "how": "left", "prefix": "client_"},
        "Rental_Items": {"on": "rental_id", "how": "left", "prefix": "item_"}
    },

    "Rental_Items": {
        "Rentals": {"on": "rental_id", "how": "inner", "prefix": "rent_"},
        "Copies": {"on": "carrier_id", "how": "inner", "prefix": "copy_"}
    },

    "Copies": {
        "Movies": {"on": "movie_id", "how": "left", "prefix": "movie_"},
        "Rental_Items": {"on": "carrier_id", "how": "left", "prefix": "item_"}
    }
}

def check_if_tables_join(table1, table2):
    return table2 in join_config[table1].keys()

def prefix_columns(df, prefix):
    return df.select([df[col].alias(prefix +'_'+ col) for col in df.columns])

def joined_tables(source_name, target_name, df1, df2, old_prefix=None, prefix=None):
    join_key = join_config[source_name][target_name]['on']
    df2 = prefix_columns(df2, prefix)
    left_key  = (old_prefix +'_'+ join_key) if old_prefix else join_key
    right_key = prefix +'_'+ join_key
    how=join_config[source_name][target_name]['how']
    agg_exprs = [F.collect_set(col).alias(col) for col in df2.columns if col != right_key]
    df2_agg = df2.groupBy(right_key).agg(*agg_exprs)
    cols_to_convert = [col for col in df2_agg.columns if col != right_key]
    for col_name in cols_to_convert:
        df2_agg = df2_agg.withColumn(col_name, F.concat_ws(", ", F.col(col_name)))
    return df1.join(df2_agg, df1[left_key] == df2_agg[right_key], how)
    
def find_join_map(table_name, joins):
    tables_to_read = [table_name] + joins
    dfs = {
        t: spark.read.table(f"workspace.silver.{t}")
        for t in tables_to_read
    }
    target_df = spark.read.table(f"workspace.silver.{table_name}")
    left_to_join = joins.copy()
    matched = {}
    for other_table in joins:
        if check_if_tables_join(table_name, other_table):
            prefix=other_table
            matched.update({other_table:prefix})
            left_to_join.remove(other_table)
            other_df = dfs[other_table]    
            target_df=joined_tables(table_name, other_table, target_df, other_df, prefix=prefix)
    while len(left_to_join)>0:
        operation_made=False
        for other_table in left_to_join[:]:
            for matched_table in list(matched.keys()):
                if check_if_tables_join(other_table, matched_table):
                    other_df = dfs[other_table]
                    prefix=matched[matched_table]
                    other_prefix=f"{other_table}_"
                    matched.update({other_table:other_prefix})
                    left_to_join.remove(other_table)
                    operation_made=True
                    target_df=joined_tables(matched_table, other_table, target_df, other_df, prefix=other_prefix, old_prefix=prefix)
        if not operation_made:
            raise e.CantMapSelectedTables(left_to_join)
    return target_df

display(find_join_map("Movies", ["Movie_Genre","Genres"]))
