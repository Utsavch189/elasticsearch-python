from elastic import es
from elasticsearch import helpers

# Create an index
es.indices.create(index="books", ignore=400)

"""
If you're creating an index and it already exists, 
Elasticsearch might return a 400 error. 
By using ignore=400, you avoid stopping your script.
This means: "Try to create the index, but don't crash if it already exists."
"""

# Index a document
doc = {
    "title": "Elasticsearch Essentials",
    "author": "Jane Doe",
    "published_date": "2023-01-15",
    "pages": 250,
    "tags": ["search", "elasticsearch", "python"]
}
es.index(index="books", id=1, document=doc) # not good way for bulk inserts

# For bulk inserts
docs = [
    {
        "title": "Learning Elasticsearch",
        "author": "John Smith",
        "published_date": "2022-09-10",
        "pages": 320,
        "tags": ["elasticsearch", "data", "search"]
    },
    {
        "title": "Advanced Python Search",
        "author": "Alice Johnson",
        "published_date": "2021-06-25",
        "pages": 275,
        "tags": ["python", "search", "algorithms"]
    },
    {
        "title": "Big Data Search Techniques",
        "author": "Carlos Ruiz",
        "published_date": "2023-03-05",
        "pages": 410,
        "tags": ["big data", "elasticsearch", "analytics"]
    },
    {
        "title": "Modern Information Retrieval",
        "author": "Emma Liu",
        "published_date": "2020-11-15",
        "pages": 390,
        "tags": ["retrieval", "search engine", "text mining"]
    },
    {
        "title": "Search Engine Fundamentals",
        "author": "Ravi Patel",
        "published_date": "2022-01-30",
        "pages": 360,
        "tags": ["search", "engine", "basics"]
    }
]

actions = [
    {
        "_index": "books",
        "_source": doc
    }
    for doc in docs
]

helpers.bulk(es, actions)