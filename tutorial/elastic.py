from elasticsearch import Elasticsearch

es = Elasticsearch(
    "http://192.168.0.115:9200",
    basic_auth=("elastic", "bwO8uM9iPzrHctxcieA2")
)