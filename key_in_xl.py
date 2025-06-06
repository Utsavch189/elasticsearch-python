import pandas as pd
from elasticsearch import Elasticsearch

index_name = "financial_data_screener"
doc_id = "132540_consolidate"

es = Elasticsearch(
    "http://192.168.0.115:9200",
    basic_auth=("elastic", "bwO8uM9iPzrHctxcieA2")
)

try:
    response = es.get(index=index_name, id=doc_id)
    document = response['_source']
except Exception as e:
    print("Error:", e)

def flatten_keys(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_keys(v, new_key, sep=sep))
        else:
            items.append(new_key)
    return items

# Flatten and extract field names
field_names = flatten_keys(document)

# Write to Excel
df = pd.DataFrame(field_names, columns=['Field Names'])
df.to_csv("field_names.csv", index=False)

print("Field names saved to field_names.xlsx")
