# Dictionary mapping for table names: {table_name: new_table_name}

table_mapping = {
    "ETLFILMY": "Movies",
    "ETLFIL_KRA": "Movie_Country",
    "ETLFIL_RODZ": "Movie_Genre",
    "ETLKLI": "Clients",
    "ETLKRAJE": "Countries",
    "ETLNOSNIKI": "Copies",
    "ETLREZYSERZY": "Directors",
    "ETLRODZAJE": "Genres",
    "ETLWYPO": "Rentals",
    "ETLWYPO_NOS": "Rental_Items"
}

def get_new_table_name(table_name):
    return table_mapping.get(table_name)