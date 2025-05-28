from elastic import es

query = {
    "size": 0,
    "aggs": {
        "top_authors": {
            "terms": {
                "field": "author.keyword"
            }
        }
    }
}

query1 = {
    "size": 0,
    "aggs": {
        "tags_count": {
            "terms": {
                "field": "tags.keyword"
            }
        }
    }
}

query2 = {
  "size": 0,
  "aggs": {
    "top_page_counts": {
      "terms": {
        "field": "pages",
        "order": { "_key": "desc" }
      }
    }
  }
}

res = es.search(index="books", body=query2)
for bucket in res["aggregations"]["top_page_counts"]["buckets"]:
    print(f"{bucket['key']}: {bucket['doc_count']}")
