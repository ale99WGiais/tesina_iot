
print("starting client...")

from socket import *
import sys

HOST, PORT = "localhost", 10002
data = "ciao come stai?\n"

import os
print(os.getcwd())


def writeFile(socket, file):
    pass

# Create a socket (SOCK_STREAM means a TCP socket)
class Client:
    def __init__(self):
        self.rbufsize = -1
        self.wbufsize = 0

    def run(self):
        with socket(AF_INET, SOCK_STREAM) as sock:
            print("connected to " + str(sock))

            #Connect to server and send data
            sock.connect((HOST, PORT))

            rfile = sock.makefile('rb', self.rbufsize)
            wfile = sock.makefile('wb', self.wbufsize)

            wfile.write(bytes(data, "utf-8"))

            with open('../small_file.txt', 'rb') as f:
                sock.writeFile(f, 0)

            sock.shutdown(SHUT_WR)

            # Receive data from the server and shut down
            received = rfile.readline().strip()

            print("Sent:     {}".format(data))
            print("Received: {}".format(received))

Client().run()