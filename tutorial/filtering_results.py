from elastic import es

query = {
    "query": {
        "bool": {
            "filter": [
                {"term": {"author.keyword": "Jane Doe"}},
                {"range": {"pages": {"gte": 200}}}
            ]
        }
    }
}
res = es.search(index="books", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])