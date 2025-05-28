from elastic import es

query = {
    "query": {
        "match": {
            "title": "Elasticsearch"
        }
    },
    "highlight": {
        "fields": {
            "title": {}
        }
    }
}
res = es.search(index="books", body=query)
for hit in res["hits"]["hits"]:
    print(hit["highlight"]["title"])
