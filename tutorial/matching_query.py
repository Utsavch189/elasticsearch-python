from elastic import es

query = {
    "query": {
        "match": {
            "title": "Elasticsearch"
        }
    }
}
res = es.search(index="books", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])