
from socketserver import *
import sqlite3
import os
import sys
import socket

NAME = "dataserver10010"
HOST = "localhost"
PORT = 10010

#set env
if len(sys.argv) > 2:
    NAME = sys.argv[1]
    PORT = int(sys.argv[2])

SERVER = str(HOST) + ":" + str(PORT)

print("dataserver " + str((NAME, HOST, PORT)))

workingDir = "../data/" + NAME
if not os.path.exists(workingDir):
    os.makedirs(workingDir)
os.chdir(workingDir)
print("work on " + str(os.getcwd()))

#create db if not exists
if not os.path.exists("database.db"):
    with sqlite3.connect('database.db') as conn:
        with open("../../dataserver/create_db.sqlite3", "r") as sql:
            conn.executescript(sql.read())

class Database:
    def __init__(self):
        try:
            self.connection = sqlite3.connect('database.db')
            self.cursor = self.connection.cursor()
            print("connected to database")

        #cursor = sqliteConnection.cursor()
          #  sqlite_select_Query = "select * from object "
          #  cursor.execute(sqlite_select_Query)
          #  record = cursor.fetchall()
          #  print("SQLite Database Version is: ", record)
          #  cursor.close()

        except sqlite3.Error as error:
            print("error while connecting to database", error)

    def getObject(self, uid):
        print("getObject ------>", (str(uid), ))
        self.cursor.execute("select * from object where uid = ?", (str(uid), ))
        return self.cursor.fetchone()

    def addObject(self, uid, local_path, size, checksum):
        complete = 0

        self.cursor.execute("insert into object(uid, local_path, size, complete, checksum) values (?, ?, ?, ?, ?)",
                            (uid, local_path, size, complete, checksum))
        self.connection.commit()

        return self.cursor.lastrowid

    def setComplete(self, uid):
        self.cursor.execute("update object set complete=1 where uid = ?", (uid, ))
        self.connection.commit()


class DataServer(ThreadingTCPServer):
    def server_activate(self):
        ThreadingTCPServer.server_activate(self)
        print("starting dataserver at " + str(self.server_address))

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

    def readfile(self, size, outFile):
        size = int(size)
        pos = 0
        while pos != size:
            chunk = min(1024, size - pos)
            # print("read", chunk, "bytes")
            data = self.rfile.read(chunk)
            outFile.write(data)
            pos += len(data)

    def sendFile(self, filepath):
        with open(filepath) as file:
            self.s.sendfile(file)

    def close(self):
        self.s.close()

class DataServerHandler(StreamRequestHandler):
    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        print("write ", res)
        self.wfile.write(res.encode())

    def readline(self):
        return self.rfile.readline().strip().decode().split(";")

    def sendFile(self, filepath):
        with open(filepath, "rb") as file:
            self.connection.sendfile(file)

    def readfile(self, size, outFile):
        size = int(size)
        pos = 0
        while pos != size:
            chunk = min(1024, size - pos)
            # print("read", chunk, "bytes")
            data = self.rfile.read(chunk)
            outFile.write(data)
            pos += len(data)

    def createUid(self, args):
        uid, size, checksum = args
        local_path = os.getcwd() + "/" + uid
        print("local_path", local_path)

        self.database.addObject(uid, local_path, size, checksum)

        self.write("ok")

    def pushUid(self, args):
        uid, = args

        objinfo = self.database.getObject(uid)

        if objinfo is None:
            self.write("ERR", "uid info not found")
            return False

        print(objinfo)

        uid, localpath, size, complete, checksum = objinfo

        with open(localpath, "wb") as out:
            self.readfile(int(size), out)

        self.database.setComplete(uid)

        metaConn = Connection(("localhost", 10000))
        metaConn.write("pushComplete", uid, SERVER)
        res = metaConn.readline()
        print(res)
        metaConn.close()

        self.write("ok")

    def transfer(self, args):
        uid, server = args

        res = self.database.getObject(uid)
        if res is None:
            self.write("err", "uid not found")
            return False

        _, localPath, _, _, _ = res

        host, port = server.split(":")
        port = int(port)
        target = Connection((host, port))
        target.write("pushUid", uid)
        print(target.readline())
        target.sendFile(localPath)
        target.close()

        self.write("ok")

    def getUid(self, args):
        uid, = args

        res = self.database.getObject(uid)

        if res is None:
            self.write("err", "specified uid not present")
            return False

        print(res)

        uid, localPath, size, complete, checksum = res

        if not complete:
            self.write("err", "not complete")
            return False

        self.write("ok", size)
        self.sendFile(localPath)

    def test(self, args):
        print("test")

        pass

    def handle(self):
        self.database = Database()

        switcher = {
            "createUid": self.createUid,
            "pushUid": self.pushUid,
            "getUid": self.getUid,
            "transfer": self.transfer,
            "test": self.test
        }

        print("handle request from " + str(self.client_address))

        data = self.readline()
        print(data)

        switcher[data[0]](data[1:])


with DataServer((HOST, PORT), DataServerHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()



