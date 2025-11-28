join_config = {
    "Movies": {
        "joins": [
            {
                "other_table": "Directors",
                "left_key": "director_id",
                "right_key": "director_id",
                "how": "left",
                "prefix": "director_"
            },
            {
                "other_table": "Movie_Country",
                "left_key": "movie_id",
                "right_key": "movie_id",
                "how": "left",
                "prefix": "mc_"
            },
            {
                "other_table": "Movie_Genre",
                "left_key": "movie_id",
                "right_key": "movie_id",
                "how": "left",
                "prefix": "mg_"
            }
        ]
    },

    "Movie_Country": {
        "joins": [
            {
                "other_table": "Countries",
                "left_key": "country_id",
                "right_key": "country_id",
                "how": "left",
                "prefix": "country_"
            }
        ]
    },

    "Movie_Genre": {
        "joins": [
            {
                "other_table": "Genres",
                "left_key": "genre_id",
                "right_key": "genre_id",
                "how": "left",
                "prefix": "genre_"
            }
        ]
    },

    "Copies": {
        "joins": [
            {
                "other_table": "Movies",
                "left_key": "movie_id",
                "right_key": "movie_id",
                "how": "left",
                "prefix": "movie_"
            }
        ]
    },

    "Rentals": {
        "joins": [
            {
                "other_table": "Clients",
                "left_key": "client_id",
                "right_key": "client_id",
                "how": "left",
                "prefix": "client_"
            }
        ]
    },

    "Rental_Items": {
        "joins": [
            {
                "other_table": "Rentals",
                "left_key": "rental_id",
                "right_key": "rental_id",
                "how": "inner",
                "prefix": "rental_"
            },
            {
                "other_table": "Copies",
                "left_key": "carrier_id",
                "right_key": "carrier_id",
                "how": "inner",
                "prefix": "copy_"
            },
            {
                "other_table": "Movies",
                "left_key": "movie_id",
                "right_key": "movie_id",
                "how": "left",
                "prefix": "movie_"
            },
            {
                "other_table": "Clients",
                "left_key": "client_id",
                "right_key": "client_id",
                "how": "left",
                "prefix": "client_"
            }
        ]
    },
    "Directors": { "joins": [] },
    "Genres":    { "joins": [] },
    "Countries": { "joins": [] },
    "Clients":   { "joins": [] }
}

def return_joins_for_table(table_name):
    return join_config[table_name]['joins']