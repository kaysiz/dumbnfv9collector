import os, socket, struct, sys
from datetime import date
from time import time
import config as cfg

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
