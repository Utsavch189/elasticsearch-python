from elastic import es

query = {
    "query": {
        "multi_match": {
            "query": "Elasticsearch Python",
            "fields": ["title", "tags"]
        }
    }
}
res = es.search(index="books", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])