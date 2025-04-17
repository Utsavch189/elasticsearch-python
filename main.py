from elasticsearch import Elasticsearch

es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("elastic", "UROjXtRa6IFY9oHYWV34")
)

print(es.ping())

# Create a cluster
# es.indices.create(index="my_keywords")

# Get all indices
indxs = es.indices.get_alias(index="*")
# for idx in indxs:
#     print(idx)

# Search a index
idx = es.search(index="my_keywords")
# print(idx)

# Search , Display, Delete index by pattern matching

## Suppose create 10 indexces for June month
# for i in range(1,11):
#     es.indices.create(index="june_"+str(i))

## Get all indexces from june
index = "june_*"
indxs = es.indices.get_alias(index=index)
# for idx in indxs:
#     print(idx)

## Delete them
# index = "june_*"
# indxs = es.indices.get_alias(index=index)
# for idx in indxs:
#     es.indices.delete(index=idx)

"""
In Elasticsearch, indices (or indexes) are the basic data structures where documents are stored. 
An index is a collection of documents that share the same data structure and are used to store data, allowing Elasticsearch to index and search efficiently. 
Each index has a set of mappings, which define the structure of the documents.

Key Concepts of Indices:
Index: A logical namespace that points to a collection of documents in Elasticsearch.

Document: A JSON object that is indexed and stored in Elasticsearch.

Shard: Each index is divided into shards. Shards are the basic unit of storage and distributed across the cluster.


"""