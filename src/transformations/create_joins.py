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

def prefix_columns(df, prefix, skip_cols=None):
    if skip_cols is None:
        skip_cols = []
    for col in df.columns:
        if col not in skip_cols:
            df = df.withColumnRenamed(col, prefix + col)
    return df

def joined_tables(source_name, target_name, df1, df2, prefix, old_prefix=None):
    join_key = join_config[source_name][target_name]['on']
    df2 = prefix_columns(df2, prefix, skip_cols=[join_key])
    on1=join_config[source_name][target_name]['on']
    how=join_config[source_name][target_name]['how']
    on2=on1
    if old_prefix is not None:
        on2=old_prefix+on1
    return df1.join(df2, df1[on2]==df2[on1], how)

def find_join_map(table_name, joins, skip_cols=None):
    target_df = spark.read.table(f"workspace.silver.{table_name}")
    left_to_join = joins.copy()
    matched = {}
    for other_table in joins:
        if check_if_tables_join(table_name, other_table):
            prefix=f"{other_table[:3]}_"
            matched.update({other_table:prefix})
            left_to_join.remove(other_table)
            other_df = spark.read.table(f"workspace.silver.{other_table}")
            target_df=joined_tables(table_name, other_table, target_df, other_df, prefix)
    while len(left_to_join)>0:
        operation_made=False
        for other_table in left_to_join:
            for matched_table in list(matched.keys()):
                if check_if_tables_join(other_table, matched_table):
                    prefix=f"{other_table[:3]}_"
                    matched.update({other_table:f"{matched[matched_table]}_{prefix}"})
                    left_to_join.remove(other_table)
                    operation_made=True
                    target_df=joined_tables(matched_table, other_table, target_df, other_df, f"{other_table[:3]}_",matched[matched_table])
        if not operation_made:
            return None
    return target_df

display(find_join_map('Rental_Items', ['Rentals', 'Copies','Movies']))