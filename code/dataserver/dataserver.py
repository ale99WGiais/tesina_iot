
from socketserver import *

class DataServer(ThreadingTCPServer):
    def server_activate(self):
        ThreadingTCPServer.server_activate(self)
        print("starting dataserver at " + str(self.server_address))

class DataServerHandler(StreamRequestHandler):
    def handle(self):
        print("handle request from " + str(self.client_address))

        data = self.rfile.readline().strip()
        self.wfile.write(data.upper())


HOST = "localhost"
PORT = 10010

with DataServer((HOST, PORT), DataServerHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()