from db import get_connection
import pandas as pd

df = pd.read_csv('field_names.csv')

fields = {}
fields_reverse = {}

con,cursor = get_connection()

for index, row in df.iterrows():
    _row = row['Field Names']
    try:
        raw_field = _row.split(".")[1]
    except:
        raw_field = None

    if raw_field:
        q = f"Select name from screener_auto_suggestions Where column_name = '{raw_field}' and data_type = 'std';"
        cursor.execute(q)
        res = cursor.fetchone()
        if res:
            fields[res[0]] = _row
            fields_reverse[_row] = res[0]

con.close()