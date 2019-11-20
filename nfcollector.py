import os, socket, struct, sys
from datetime import date
from time import time
import config as cfg
from es import createIndex, createFlow, _es





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

setBody = {
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "sysUptimeFirst": {
                "type": "text"
            },
            "sysUptimeLast": {
                "type": "text"
            },
            "counterBytes": {
                "type": "text"
            },
            "counterPackets": {
                "type": "text"
            },
            "inputInterface": {
                "type": "text"
            },
            "outputInterface": {
                "type": "text"
            },
            "ipv4SrcAddr": {
                "type": "text"
            },
            "ipv4DstAddr": {
                "type": "text"
            },
            "ipProtocol": {
                "type": "text"
            },
            "ipTos": {
                "type": "text"
            },
            "transportSrcPort": {
                "type": "text"
            },
            "transportDstPort": {
                "type": "text"
            },
            "flowSampler": {
                "type": "text"
            },
            "ipv4NextHop": {
                "type": "text"
            },
            "ipv4DstMask": {
                "type": "text"
            },
            "ipv4SrcMask": {
                "type": "text"
            },
            "tcpFlags": {
                "type": "text"
            },
            "destinationAS": {
                "type": "text"
            },
            "sourceAS": {
                "type": "text"
            }
        }
    }
}


# if __name__ == "__main__":
#     createIndex(_es, "netflow-v9", setBody)

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
                #print(firstFlow)
                createFlow(_es, 'netflow-v9', {"sysUptimeFirst": firstFlow[0], "sysUptimeLast": firstFlow[1], "counterBytes": firstFlow[2], \
                    "counterPackets": firstFlow[3], "inputInterface": firstFlow[4], "outputInterface": firstFlow[5], "ipv4SrcAddr": firstFlow[6], \
                        "ipv4DstAddr": firstFlow[7], "ipProtocol": firstFlow[8], "ipTos": firstFlow[9], "transportSrcPort": firstFlow[10], \
                            "transportDstPort": firstFlow[11], "flowSampler": firstFlow[12], "ipv4NextHop": firstFlow[13], "ipv4DstMask": firstFlow[14], \
                                "ipv4SrcMask": firstFlow[15], "tcpFlags": firstFlow[16], "destinationAS": firstFlow[17], "sourceAS": firstFlow[18]})
            else:
                offset = flow * templSize
                subseqFlow = struct.unpack('!IIIIIIIIBBHHBIBBBHH', data[24 + offset:74 + offset])
                createFlow(_es, 'netflow-v9', {"sysUptimeFirst": firstFlow[0], "sysUptimeLast": firstFlow[1], "counterBytes": firstFlow[2], \
                    "counterPackets": firstFlow[3], "inputInterface": firstFlow[4], "outputInterface": firstFlow[5], "ipv4SrcAddr": firstFlow[6], \
                        "ipv4DstAddr": firstFlow[7], "ipProtocol": firstFlow[8], "ipTos": firstFlow[9], "transportSrcPort": firstFlow[10], \
                            "transportDstPort": firstFlow[11], "flowSampler": firstFlow[12], "ipv4NextHop": firstFlow[13], "ipv4DstMask": firstFlow[14], \
                                "ipv4SrcMask": firstFlow[15], "tcpFlags": firstFlow[16], "destinationAS": firstFlow[17], "sourceAS": firstFlow[18]})
                #print(subseqFlow)

if __name__ == '__main__':
    startCapture(runMode)
    #s.close()