join_config = {

    "Movies": {
        "joins": [
            {"other": "Directors",     "on": "director_id", "how": "left", "prefix": "dir_"},
            {"other": "Movie_Country", "on": "movie_id",    "how": "left", "prefix": "mc_"},
            {"other": "Movie_Genre",   "on": "movie_id",    "how": "left", "prefix": "mg_"},
            {"other": "Copies",        "on": "movie_id",    "how": "left", "prefix": "copy_"}
        ]
    },

    "Directors": {
        "joins": [
            {"other": "Movies", "on": "director_id", "how": "left", "prefix": "movie_"}
        ]
    },

    "Countries": {
        "joins": [
            {"other": "Movie_Country", "on": "country_id", "how": "left", "prefix": "mc_"}
        ]
    },

    "Movie_Country": {
        "joins": [
            {"other": "Movies",    "on": "movie_id",   "how": "left", "prefix": "movie_"},
            {"other": "Countries", "on": "country_id", "how": "left", "prefix": "country_"}
        ]
    },

    "Genres": {
        "joins": [
            {"other": "Movie_Genre", "on": "genre_id", "how": "left", "prefix": "mg_"}
        ]
    },

    "Movie_Genre": {
        "joins": [
            {"other": "Movies", "on": "movie_id", "how": "left", "prefix": "movie_"},
            {"other": "Genres", "on": "genre_id", "how": "left", "prefix": "genre_"}
        ]
    },

    "Clients": {
        "joins": [
            {"other": "Rentals", "on": "client_id", "how": "left", "prefix": "rent_"}
        ]
    },

    "Rentals": {
        "joins": [
            {"other": "Clients",      "on": "client_id", "how": "left", "prefix": "client_"},
            {"other": "Rental_Items", "on": "rental_id", "how": "left", "prefix": "item_"}
        ]
    },

    "Rental_Items": {
        "joins": [
            {"other": "Rentals", "on": "rental_id",  "how": "inner", "prefix": "rent_"},
            {"other": "Copies",  "on": "carrier_id", "how": "inner", "prefix": "copy_"}
        ]
    },

    "Copies": {
        "joins": [
            {"other": "Movies",       "on": "movie_id",   "how": "left", "prefix": "movie_"},
            {"other": "Rental_Items", "on": "carrier_id", "how": "left", "prefix": "item_"}
        ]
    }
}

def return_joins_for_table(table_name):
    return join_config[table_name]['joins']

def find_join_map(table_name, joins):
    matched=join_config[table_name]['joins']['other_table']
    for join in joins:
        if join["table"] == table_name:
            return join
    return None