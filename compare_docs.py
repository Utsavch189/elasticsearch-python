import pandas as pd
from elasticsearch import Elasticsearch

index_name = "financial_data_screener"
doc_id_cons = "132540_consolidate"
doc_id_std = "132540_standalone"

es = Elasticsearch(
    "http://192.168.0.115:9200",
    basic_auth=("elastic", "bwO8uM9iPzrHctxcieA2")
)

try:
    response_std = es.get(index=index_name, id=doc_id_std)
    document_std = response_std['_source']

    response_cons = es.get(index=index_name, id=doc_id_cons)
    document_cons = response_cons['_source']
except Exception as e:
    print("Error:", e)

def flatten_keys(d, parent_key='', sep='.'):
    keys = set()
    for k, v in d.items():
        full_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            keys.update(flatten_keys(v, full_key, sep=sep))
        else:
            keys.add(full_key)
    return keys


# Flatten keys
keys1 = flatten_keys(document_std)
keys2 = flatten_keys(document_cons)

# Print totals
print(f"Total keys in standalone: {len(keys1)}")
print(f"Total keys in consolidate: {len(keys2)}")

# Compare keys
only_in_dict1 = keys1 - keys2
only_in_dict2 = keys2 - keys1

print("\nKeys only in standalone:")
for key in sorted(only_in_dict1):
    print("-", key)

print("\nKeys only in consolidate:")
for key in sorted(only_in_dict2):
    print("-", key)

