from elastic import es

#If your documents contain location data, perform geo-based searches:

query = {
    "query": {
        "geo_distance": {
            "distance": "50km",
            "location": {
                "lat": 40.7128,
                "lon": -74.0060
            }
        }
    }
}
res = es.search(index="places", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])