from elastic import es

query = {
    "query": {
        "bool": {
            "must": [
                {"range": {"ratios.roe": {"gt": 10}}},
                {"range": {"growth.revenue_growth": {"lt": 8}}}
            ]
        }
    }
}

if __name__ == "__main__":
    response = es.search(index="financial_data", body=query)
    for hit in response["hits"]["hits"]:
        print(hit["_source"])