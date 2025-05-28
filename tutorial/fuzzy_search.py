from elastic import es

query = {
    "query": {
        "fuzzy": {
            "title": {
                "value": "Elastcsearch",
                "fuzziness": "AUTO"
            }
        }
    }
}
res = es.search(index="books", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])