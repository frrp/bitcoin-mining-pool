#!/usr/bin/env python
# Send notification to all connected clients

import socket
import json
import sys
import argparse
import time

start = time.time()

parser = argparse.ArgumentParser(description='Reconnect all connected nodes to another host.')
parser.add_argument('--password', dest='password', type=str, help='Use admin password from Stratum server config')
parser.add_argument('--to-host', dest='to_host', type=str, help='Host to connect')
parser.add_argument('--to-port', dest='to_port', type=int, help='Port to connect')
parser.add_argument('--host', dest='host', type=str, default='localhost', help='Hostname of Stratum mining instance')
parser.add_argument('--port', dest='port', type=int, default=3333, help='Port of Stratum mining instance')

args = parser.parse_args()

if args.password == None:
	parser.print_help()
	sys.exit()
	
message = {'id': 1, 'method': 'admin.send_reconnect', 'params': [args.password, args.to_host, args.to_port]}

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args.host, args.port))
    s.sendall(json.dumps(message)+"\n")
    data = s.recv(16000)
    s.close()
except IOError:
    print "blocknotify: Cannot connect to the pool"
    sys.exit()

for line in data.split("\n"):
    if not line.strip():
    	# Skip last line which doesn't contain any message
        continue

    message = json.loads(line)
    if message['id'] == 1:
        if message['result'] == True:
	        print "send_message: done in %.03f sec" % (time.time() - start)
        else:
            print "send_message: Error during request:", message['error'][1]
    elif message['id'] == None and message['method'] == 'client.reconnect':
        print "send_message: Received notification:", message['params'][0]
    else:
        print "send_message: Unexpected message from the server:", message
