from elastic import es

query = {
    "query": {
        "script_score": {
            "query": {
                "match": {
                    "title": "Elasticsearch"
                }
            },
            "script": {
                "source": "doc['pages'].value / 10"
            }
        }
    }
}
res = es.search(index="books", body=query)
print(res)
for hit in res["hits"]["hits"]:
    print(hit["_source"])