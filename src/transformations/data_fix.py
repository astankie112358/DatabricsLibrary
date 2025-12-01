from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def replace_polish_characters(df):
    polish_map = {
        'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n',
        'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
        'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N',
        'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z'
    }
    def replace_chars(s):
        if s is None:
            return None
        for k, v in polish_map.items():
            s = s.replace(k, v)
        return s

    replace_udf = udf(replace_chars, StringType())
    for col_name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col_name, replace_udf(df[col_name]))
    return df