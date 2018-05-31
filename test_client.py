import sys
import json
import time
import socket
import hashlib


HOST, PORT = "174.129.148.33", 9999
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(10)
username = '1957295fdsahjklbfdjk'
email = 'tsyhfndasklfds@dsalkbga.com'
uid = hashlib.md5(username.encode('utf-8')).hexdigest()

def run_test(fn):
    try:
        fn()
        return True
    except Exception:
        return False
    

def test_echo():
    data = {}
    data['cmd'] = 'echo'
    data['useless'] = 'thisisauselessstringofinfromationthatwillberemoved'
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = str(sock.recv(1024), "utf-8")

    print("Sent:     {}".format(data))
    print("Received: {}".format(received))

def add_user():
    data = {}
    data['cmd'] = 'add_user'
    data['mcusername'] = username
    data['email'] = email

    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = str(sock.recv(1024), "utf-8")

    print("Sent:     {}".format(data))
    print("Received: {}".format(received))

def remove_user():
    data = {}
    data['cmd'] = 'remove_user'
    data['uid'] = uid

    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = str(sock.recv(1024), "utf-8")

    print("Sent:     {}".format(data))
    print("Received: {}".format(received))

def get_firehose_key():
    data = {}
    data['cmd'] = 'get_firehose_key'
    data['uid'] = uid

    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = str(sock.recv(1024), "utf-8")

    print("Sent:     {}".format(data))
    print("Received: {}".format(received))   
    

def get_minecraft_key():
    data = {}
    data['cmd'] = 'get_minecraft_key'
    data['uid'] = uid

    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = str(sock.recv(1024), "utf-8")

    print("Sent:     {}".format(data))
    print("Received: {}".format(received))


while True:

    # Ping the user server with echo command
    run_test(test_echo)

    # Add a user
    run_test(add_user)

    # Get a firehose key for that user

    # Get a minecraft key for that user
    run_test(get_minecraft_key)

    # Remove that user
    run_test(remove_user)

    



    time.sleep(1)