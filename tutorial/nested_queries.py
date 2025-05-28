from elastic import es

query = {
    "query": {
        "nested": {
            "path": "reviews",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"reviews.reviewer": "Alice"}},
                        {"range": {"reviews.rating": {"gte": 4}}}
                    ]
                }
            }
        }
    }
}
res = es.search(index="books", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])