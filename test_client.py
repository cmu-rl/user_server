import sys
import json
import time
import socket


HOST, PORT = "localhost", 9999


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


data = {}
data['cmd'] = 'echo'
data['useless'] = 'thisisauselessstringofinfromationthatwillberemovedinther\
    esponcebecausetheechocommandwillonlyreturnthecommandsenttotheserver'

while True:
    sock.sendto(json.dumps(data), (HOST, PORT))
    received = str(sock.recv(1024), "utf-8")

    print("Sent:     {}".format(data))
    print("Received: {}".format(received))

    time.sleep(1)