

from socketserver import *

class MetaServer(ThreadingTCPServer):
    def server_activate(self):
        ThreadingTCPServer.server_activate(self)
        print("starting metaserver at " + str(self.server_address))

class MetaServerHandler(StreamRequestHandler):
    def handle(self):
        print("handle request from " + str(self.client_address))

        data = self.rfile.readline().strip()
        self.wfile.write(data.upper())


HOST = "localhost"
PORT = 10000



with MetaServer((HOST, PORT), MetaServerHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()