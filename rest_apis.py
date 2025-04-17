import requests

# Get all index
res = requests.get("http://localhost:9200/_cat/indices?format=json",auth=("elastic", "UROjXtRa6IFY9oHYWV34"))
# print(res.json())

# Create index
# index_name = "utsav1"
# res = requests.put(f"http://localhost:9200/{index_name}",auth=("elastic", "UROjXtRa6IFY9oHYWV34"))
# print(res.text)

# Get a index
index_name = "utsav1"
res = requests.get(f"http://localhost:9200/{index_name}?format=json",auth=("elastic", "UROjXtRa6IFY9oHYWV34"))
# print(res.json())

# Delete a index
index_name = "utsav1"
res = requests.delete(f"http://localhost:9200/{index_name}?format=json",auth=("elastic", "UROjXtRa6IFY9oHYWV34"))
# print(res.json())


# Add a document to an index
index_name = "my_keywords"
headers = {
    "Content-Type":"application/json"
}
data = {
    "message":"hello"
}
res = requests.put(f"http://localhost:9200/{index_name}/_doc/1?format=json",headers=headers,json=data,auth=("elastic", "UROjXtRa6IFY9oHYWV34"))
print(res.text)