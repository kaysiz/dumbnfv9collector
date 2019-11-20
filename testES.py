from elasticsearch import Elasticsearch
import logging

def connectES():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Ping ok')
    else:
        print('Ping error')
    return _es

def createIndex(esObject, esIndexName='recipes'):
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properies": {
                "title": {"type": "text"},
                "sub_title": {"type": "text"},
                "seq_number": {"type": "integer"}
            }
        }
    }
    try:
        if not esObject.indices.exists(esIndexName):
            esObject.indices.create(index=esIndexName, ignore=400, body=settings)
            print('Index created')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    es = connectES()
    es
    #createIndex(es)