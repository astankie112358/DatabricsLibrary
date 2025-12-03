import src.errors.custom_errors as e
import pyspark.sql.functions as F

join_config = {
    "movies": {
        "directors": {"on": "director_id", "how": "left", "prefix": "dir_"},
        "movie_country": {"on": "movie_id", "how": "left", "prefix": "mc_"},
        "movie_genre": {"on": "movie_id", "how": "left", "prefix": "mg_"},
        "copies": {"on": "movie_id", "how": "left", "prefix": "copy_"}
    },

    "directors": {
        "movies": {"on": "director_id", "how": "left", "prefix": "movie_"}
    },

    "countries": {
        "movie_country": {"on": "country_id", "how": "left", "prefix": "mc_"}
    },

    "movie_country": {
        "movies": {"on": "movie_id", "how": "left", "prefix": "movie_"},
        "countries": {"on": "country_id", "how": "left", "prefix": "country_"}
    },

    "genres": {
        "movie_genre": {"on": "genre_id", "how": "left", "prefix": "mg_"}
    },

    "movie_genre": {
        "movies": {"on": "movie_id", "how": "left", "prefix": "movie_"},
        "genres": {"on": "genre_id", "how": "left", "prefix": "genre_"}
    },

    "clients": {
        "rentals": {"on": "client_id", "how": "left", "prefix": "rent_"}
    },

    "rentals": {
        "clients": {"on": "client_id", "how": "left", "prefix": "client_"},
        "rental_items": {"on": "rental_id", "how": "left", "prefix": "item_"}
    },

    "rental_items": {
        "rentals": {"on": "rental_id", "how": "inner", "prefix": "rent_"},
        "copies": {"on": "carrier_id", "how": "inner", "prefix": "copy_"}
    },

    "copies": {
        "movies": {"on": "movie_id", "how": "left", "prefix": "movie_"},
        "rental_items": {"on": "carrier_id", "how": "left", "prefix": "item_"}
    }
}

def check_if_tables_join(table1, table2):
    return table2 in join_config[table1].keys()

def prefix_columns(df, prefix):
    return df.select([df[col].alias(f"{prefix}_{col}") for col in df.columns])

def joined_tables(source_name, target_name, df1, df2, old_prefix=None, prefix=None):
    join_key = join_config[source_name][target_name]['on']
    df2 = prefix_columns(df2, prefix)
    left_key  = f"{old_prefix}_{join_key}" if old_prefix else join_key
    right_key = f"{prefix}_{join_key}"
    how=join_config[source_name][target_name]['how']
    return df1.join(
        df2,
        df1[left_key] == df2[right_key],
        how
    )

def aggregate_tables(df, matched, main_table):
    main_cols = [col for col in df.columns if not any(col.startswith(prefix + "_") for prefix in matched.values())]
    added_cols = [col for col in df.columns if any(col.startswith(prefix + "_") for prefix in matched.values())]
    return df.groupBy(main_cols).agg(*[F.regexp_replace(
                F.collect_set(col).cast("string"),
                r"[\[\]]", ""
            ).alias(col)
            for col in added_cols
        ])

def remove_columns(df, columns, matched):
    cols_to_remove = set()
    for table, prefix in matched.items():
        for col in columns:
            prefixed_col = f"{prefix}_{col}"
            if prefixed_col in df.columns:
                cols_to_remove.add(prefixed_col)
    return df.select([col for col in df.columns if col not in cols_to_remove])

def find_join_map(df, table_name, joins, drop_columns=None):
    tables_to_read = [table_name] + list(joins.keys())
    dfs = {table_name: df}
    dfs.update(joins)
    target_df = df
    left_to_join = list(joins.keys())
    matched = {}
    df = df.toDF(*[f"{table_name}_{col}" for col in df.columns])
    for other_table in left_to_join[:]:
        if check_if_tables_join(table_name, other_table):
            prefix = other_table
            matched.update({other_table: prefix})
            left_to_join.remove(other_table)
            other_df = dfs[other_table]
            target_df = joined_tables(table_name, other_table, target_df, other_df, prefix=prefix)
    while len(left_to_join) > 0:
        operation_made = False
        for other_table in left_to_join[:]:
            for matched_table in list(matched.keys()):
                if check_if_tables_join(other_table, matched_table):
                    other_df = dfs[other_table]
                    prefix = matched[matched_table]
                    other_prefix = f"{other_table}"
                    matched.update({other_table: other_prefix})
                    left_to_join.remove(other_table)
                    operation_made = True
                    target_df = joined_tables(matched_table, other_table, target_df, other_df, prefix=other_prefix, old_prefix=prefix)
        if not operation_made:
            raise e.CantMapSelectedTables(left_to_join)
    target_df = aggregate_tables(target_df, matched, table_name)
    if drop_columns:
        target_df = remove_columns(target_df, drop_columns, matched)
    return target_df