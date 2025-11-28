# Dictionary mapping for column renames: {table_name: {old_col: new_col, ...}}
column_rename_map = {
    "ETLKRAJE": {
        "IDKRAJ": "country_id",
        "KRAJPROD": "production_country"
    },
    "ETLNOSNIKI": {
        "IDNOSNIK": "carrier_id",
        "IDFILM": "movie_id",
        "STATUS": "status"
    },
    "ETLREZYSERZY": {
        "IDREZYSER": "director_id",
        "NAZWISKO": "surname",
        "IMIE": "first_name"
    },
    "ETLRODZAJE": {
        "IDRODZAJ": "genre_id",
        "RODZAJFIL": "film_genre"
    },
    "ETLWYPO": {
        "IDWYPO": "rental_id",
        "IDKLIENT": "client_id",
        "DATAW": "rental_date",
        "DATAZ": "return_date",
        "ILE_NOSNIKOW": "number_of_carriers",
        "KWOTA": "amount"
    },
    "ETLWYPO_NOS": {
        "IDWYPO": "rental_id",
        "IDNOSNIK": "carrier_id"
    },
    "ETLFIL_KRA": {
        "IDFILM": "movie_id",
        "IDKRAJ": "country_id"
    },
    "ETLFIL_RODZ": {
        "IDFILM": "movie_id",
        "IDRODZAJ": "genre_id"
    },
    "ETLFILMY": {
        "IDFILM": "movie_id",
        "TYTUL": "title",
        "IDREZYSER": "director_id",
        "CENA": "price",
        "KOLOR": "color",
        "CZAS_TRWANIA": "duration",
        "OPIS": "description"
    },
    "ETLKLI": {
        "IDKLIENT": "client_id",
        "NAZWISKO": "surname",
        "IMIE": "first_name",
        "WIEK": "age",
        "ADRES": "address",
        "TELEFON": "phone",
        "PLEC": "gender"
    }
}

def rename_columns(df, table_name):
    mapping=column_rename_map[f"{table_name}"]
    for old_col, new_col in mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df