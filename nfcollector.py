import os, socket, struct, sys
from datetime import date
from time import time
import config as cfg
from elasticsearch import Elasticsearch

es_index_settings = {
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 0
    },
    "mappings" : {
        "flows" : {
            "dynamic" : "strict",
            "properties" : {
                "version": {
                    "type" : "integer"
                },
                "count" : {
                    "type" : "integer"
                },
                "sysUpTime" : {
                    "type" : "integer"
                },
                "unixSec" : {
                    "type" : "integer"
                },
                "sequenceNumber" : {
                    "type" : "integer"
                },
                "sourceId" : {
                    "type" : "integer"
                },
                "timeStampFirst" : {
                    "type" : "integer"
                },
                "timeStampLast" : {
                    "type" : "integer"
                },
                "counterBytes" : {
                    "type" : "integer"
                },
                "counterPackets" : {
                    "type" : "integer"
                },
                "interfaceInput" : {
                    "type" : "integer"
                },
                "interfaceOutput" : {
                    "type" : "integer"    
                },
                "ipv4SrcAddr" : {
                    "type" : "integer"
                },
                "ipv4DstAddr" : {
                    "type" : "integer"
                },
                "ipProtocol" : {
                    "type" : "integer"
                },
                "ipTos" : {
                    "type" : "integer"
                },
                "transportSrcPort" : {
                    "type" : "integer"
                },
                "transportDstPort" : {
                    "type" : "integer"
                },
                "flowSampler" : {
                    "type" : "integer"
                },
                "nextHopIpv4Addr" : {
                    "type" : "integer"
                },
                "ipv4DstMask" : {
                    "type" : "integer"
                },
                "ipv4SrcMask" : {
                    "type" : "integer"
                },
                "tcpFlag" : {
                    "type" : "integer"
                },
                "destinationAS" : {
                    "type" : "integer"
                },
                "sourceAS" : {
                    "type" : "integer"
                }
            }
        }
    }
}

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.indices.create(index='netflow-v9', ignore=400, body=es_settings)

runMode = cfg.mode
ipAddress = cfg.ip_address
port = cfg.port
templSize = cfg.template_size_in_bytes
captDur = time() + cfg.caption_duration

# Init socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# Binding
s.bind((ipAddress, port))

# Date stamps
td = str(date.today())

def startCapture(mode):
    if not os.path.exists('dumps') and mode == 'raw':
        os.mkdir('dumps')

    if not os.path.exists('flows') and mode == 'unpack':
        os.mkdir('flows')

    if os.path.exists('dumps') and mode == 'raw':
        os.chdir('dumps')
        #dump = open(td + '.dump', 'wb')
    elif os.path.exists('flows') and mode == 'unpack':
        os.chdir('flows')
        #ff = open(td + '-flows.txt', 'w')

    while mode == 'raw' and time() < captDur:
        data = s.recv(1518)
        print(data)

    while mode == 'unpack' and time() < captDur:
        data = s.recv(1518)
        nfHeader = struct.unpack('!HHLLLL', data[0:20])
        #print(nfHeader)

        for flow in range(0, nfHeader[1]):
            if flow == 0:
                firstFlow = struct.unpack('!IIIIIIIIBBHHBIBBBHH', data[24:74])
                print(firstFlow)
            else:
                offset = flow * templSize
                subseqFlow = struct.unpack('!IIIIIIIIBBHHBIBBBHH', data[24 + offset:74 + offset])
                print(subseqFlow)

if __name__ == '__main__':
    startCapture(runMode)
    s.close()
