

from socketserver import *
from cassandra.cluster import Cluster
from uuid import uuid4, UUID
from datetime import datetime
import socket

#res = session.execute("select * from system.local;").all()
#print(res)


class Database:
    def __init__(self):
        self.cluster = Cluster()

        self.session = self.cluster.connect("metaserver")
        self.statements = {}
        self.statements["addObject"] = self.session.prepare(
            "insert into object(uid, path, created, owner, size, min_copies) values (?, ?, ?, ?, ?, ?)")
        self.statements["addSharedObject"] = self.session.prepare(
            "insert into stored_object(id, uid, server, created, complete) values (?, ?, ?, ?, ?)")
        self.statements["listFilter"] = self.session.prepare("select uid, path from object where path like ?")
        self.statements["list"] = self.session.prepare("select uid, path from object")
        #print(self.session)

    def addObject(self, uid, path, owner, size, min_copies):
        created = datetime.now()
        size = int(size)
        min_copies = int(min_copies)
        self.session.execute(self.statements["addObject"], (uid, path, created, owner, size, min_copies))
        return uid

    def addStoredObject(self, uid, server):
        #uid = UUID(uid)
        id = uuid4()
        created = datetime.now()
        complete = False
        self.session.execute(self.statements["addSharedObject"], (id, uid, server, created, complete))
        return id

    def list(self, path):
        if path == "":
            return self.session.execute(self.statements["list"]).all()
        else:
            path = path + "%"
            return self.session.execute(self.statements["listFilter"], (path, )).all()


class MetaServer(ThreadingTCPServer):
    def server_activate(self):
        ThreadingTCPServer.server_activate(self)
        print("starting metaserver at " + str(self.server_address))

class Connection:
    def __init__(self, endpoint):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(endpoint)

        self.wfile = self.s.makefile('wb', 0)
        self.rfile = self.s.makefile('rb', -1)

    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        print("write" , res)
        self.wfile.write(res.encode())

    def readline(self):
        return self.rfile.readline().strip().decode().split(";")

    def readfile(self, len):
        data = self.rfile.read(len)
        print(data)

    def sendFile(self, filepath):
        with open(filepath) as file:
            self.s.sendfile(file)

    def close(self):
        self.s.close()


class MetaServerHandler(StreamRequestHandler):
    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        print("write", res)
        self.wfile.write(res.encode())

    def readline(self):
        return self.rfile.readline().strip().decode().split(";")

    def readfile(self, len):
        data = self.rfile.read(len)
        print(data)

    def getPath(self, args):
        uid = args[0]
        #object = self.database.getPath(uid)

    def pushPath(self, args):
        path, size, checksum = args
        uid = uuid4()
        addr = ("localhost", 10010)

        dataServer = Connection(addr)
        dataServer.write("createUid", uid, size, checksum)
        response = dataServer.readline()
        dataServer.close()

        if response[0] != "ok":
            print("ERROR", response[0])
            self.write("err", response)
            return False

        self.database.addObject(uid, path, "root", size, 1)
        self.database.addStoredObject(uid, "localhost:10010")

        addr = "localhost:10010"
        self.write("ok", uid, addr)

    def list(self, args):
        path, = args
        res = self.database.list(path)
        print(res)
        self.write("ok", len(res))
        for line in res:
            self.write(line[0], line[1])

    def handle(self):
        self.database = Database()

        switcher = {
            "getPath": self.getPath,
            "pushPath": self.pushPath,
            "list": self.list
        }

        print("handle request from " + str(self.client_address))

        data = self.readline()
        print(data)

        switcher[data[0]](data[1:])


HOST = "localhost"
PORT = 10000

with MetaServer((HOST, PORT), MetaServerHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()