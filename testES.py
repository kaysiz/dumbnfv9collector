from elasticsearch import elasticsearch
import logging

def connectES():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Ping ok')
    else:
        print('Ping error')
    return _es

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    connectES()
