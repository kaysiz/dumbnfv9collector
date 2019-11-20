from elasticsearch import Elasticsearch
import logging

def createIndex(esObject, indexName='recipes'):
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
        if not esObject.indices.exists(indexName):
            esObject.indices.create(index=indexName, ignore=400, body=settings)
            print('Index created')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
createIndex(es)

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)