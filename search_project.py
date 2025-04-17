from elasticsearch import Elasticsearch,helpers
import csv
from flask import Flask

app = Flask(__name__)

es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("elastic", "UROjXtRa6IFY9oHYWV34")
)

def seed_keywords():
    index_name = "my_keywords"

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    data = []
    with open("screener_auto_suggestions.csv","r") as f:
        reader = csv.reader(f)
        for row in reader:
            data.append({
            "name":row[1],
            "desc":row[4]
        })
    
    mapping = {
        "mappings": {
            "properties": {
                "name": {"type": "text"},
                "desc": {"type": "text"}
                }
            }
        }
    es.indices.create(
    index=index_name,
    body={
        "settings": {
            "analysis": {
                "filter": {
                    "synonym_filter": {
                        "type": "synonym",
                        "synonyms": [
                            "eps, earnings per share, earning per share"
                        ]
                    }
                },
                "analyzer": {
                    "synonym_analyzer": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "synonym_filter"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "name": {
                    "type": "text",
                    "analyzer": "synonym_analyzer"
                },
                "description": {
                    "type": "text",
                    "analyzer": "synonym_analyzer"
                }
            }
        }
    }
)

    actions = [
        {
            "_index": index_name,
            "_source": doc
        }
        for doc in data
    ]
    helpers.bulk(es, actions)


def search_keywords(query,limit=20):
    body = {
        "size": limit,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["name","desc"]
            }
        }
    }

    res = es.search(index="my_keywords", body=body)
    data = []
    
    for hit in res["hits"]["hits"]:
        data.append({
            "score":f"{hit['_score']:.2f}",
            "name": hit['_source']['name'],
            "desc": hit['_source']['desc']
        })
    
    return data

@app.route("/search/<string:keyword>",methods=['GET'])
def search(keyword):
    return search_keywords(keyword)

if __name__=="__main__":
    # search_keywords("net")
    # seed_keywords()
    app.run(host="0.0.0.0",port=8000,debug=True)