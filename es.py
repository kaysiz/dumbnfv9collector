from elasticsearch import Elasticsearch
import logging

_es = Elasticsearch([{"host": "localhost", "port": 9200}])

def connectES():
    _es = None
    _es = Elasticsearch([{"host": "localhost", "port": 9200}])
    if _es.ping():
        print('Pinged')
    else:
        print('No ping')
    return _es

def createIndex(esObject, indexName, indexSettings):
    created = False
    try:
        if not esObject.indices.exists(indexName):
            esObject.indices.create(index=indexName, ignore=400, body=indexSettings)
            print('Index created')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
