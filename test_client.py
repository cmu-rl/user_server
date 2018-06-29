import sys
import json
import time
import socket
import hashlib


HOST, PORT = "localhost", 9999
#HOST, PORT = "184.73.82.23", 9999
#HOST, PORT = "52.91.188.21", 9999

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(2)
username = '1957295fdsahjklbfdjk'
email = 'tsyhfndasklfds@dsalkbga.com'
uid = hashlib.md5(username.encode('utf-8')).hexdigest()

def run_test(fn):
    try:
        fn()
        return True
    except Exception as e:
        print(e)
        return False
    

def test_echo():
    data = {}
    data['cmd'] = 'echo'
    data['useless'] = 'thisisauselessstringofinfromationthatwillberemoved'
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def list_users():
    data = {}
    data['cmd'] = 'list_users'

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def add_user():
    data = {}
    data['cmd'] = 'add_user'
    data['mcusername'] = username
    data['email'] = email

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def remove_user():
    data = {}
    data['cmd'] = 'remove_user'
    data['uid'] = uid

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def get_status():
    data = {}
    data['cmd'] = 'get_status'
    data['uid'] = uid

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def get_invalid_status():
    data = {}
    data['cmd'] = 'get_status'
    data['uid'] = uid + "__fake"

    print("Testing non-existent user: ")
    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def make_awesome():
    data = {}
    data['cmd'] = 'make_awesome'
    data['uid'] = uid

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

def get_firehose_key():
    data = {}
    data['cmd'] = 'get_firehose_key'
    data['uid'] = uid

    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))   
    

def get_minecraft_key_and_validate():
    data = {}
    data['cmd'] = 'get_minecraft_key'
    data['uid'] = uid

    print("Getting Key: ")
    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

    if 'minecraft_key' in received:
        print("Validating Key: ")

        key = received['minecraft_key']
        data = {}
        data['cmd'] = 'validate_minecraft_key'
        data['uid'] = uid
        data['minecraft_key'] = key


        print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
        sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
        received = json.loads(str(sock.recv(1024), "utf-8"))
        print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

    else:
        print("Not Validating Key - no minecraft_key in response")

def get_firehose_key_and_return():
    data = {}
    data['cmd'] = 'get_firehose_key'
    data['uid'] = uid


    print("Getting Firehose Stream: ")
    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))


    if 'stream_name' in received:
        print("Returning Stream: ")

        name = received['stream_name']
        data = {}
        data['cmd'] = 'return_firehose_key'
        data['uid'] = uid
        data['stream_name'] = name
        
        print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
        sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
        received = json.loads(str(sock.recv(1024), "utf-8"))
        print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

    else:
        print("Not Returning Key - no stream_name in response")


def get_firehose_key_and_disconnect():
    data = {}
    data['cmd'] = 'get_firehose_key'
    data['uid'] = uid

    print("Getting Firehose Stream: ")
    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))

    
    print("Disconnecting User: ")

    data = {}
    data['cmd'] = 'disconnect_user'
    data['uid'] = uid
    
    print("Sent:     {}".format(json.dumps(data, indent=4, sort_keys=True)))
    sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
    received = json.loads(str(sock.recv(1024), "utf-8"))
    print("Received: {}".format(json.dumps(received, indent=4, sort_keys=True)))
    


run = True
while run:

    # Ping the user server with echo command
    run_test(test_echo)
    print()

    # Add a user
    run_test(add_user)
    print()

    # Look at their status
    run_test(get_status)
    print()

    # Make that user Awesome
    run_test(make_awesome)
    print()

    # Look at their status
    run_test(get_status)
    print()

    # Get a minecraft key for that user and validate it
    run_test(get_minecraft_key_and_validate)
    print()

    # Get a firehose stream from the pool and return it
    run_test(get_firehose_key_and_return)
    print()

    
    # Get a firehose stream from the pool and disconnect them
    run_test(get_firehose_key_and_disconnect)
    print()


    # Remove that user
    run_test(remove_user)
    print()

    # Try to get a status from an invalid user
    run_test(get_invalid_status)
    print()



    run_test(list_users)
    print()

    
    run = False
    time.sleep(1)