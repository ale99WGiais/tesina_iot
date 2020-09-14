
from socketserver import *
import sqlite3
import os
import sys

NAME = "dataserver1"
HOST = "localhost"
PORT = 10010

if len(sys.argv) > 2:
    NAME = sys.argv[1]
    PORT = int(sys.argv[2])

print("dataserver " + str((NAME, HOST, PORT)))

os.chdir("../data/" + NAME)
print("work on " + str(os.getcwd()))

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

class DataServerHandler(StreamRequestHandler):
    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        print("write ", res)
        self.wfile.write(res.encode())

    def readline(self):
        return self.rfile.readline().strip().decode().split(";")

    def readfile(self, len):
        data = self.rfile.read(len)
        print(data)
        return data

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

        data = self.readfile(int(size))

        with open(localpath, "wb") as out:
            out.write(data)

        self.database.setComplete(uid)

        self.write("ok")

    def handle(self):
        self.database = Database()

        switcher = {
            "createUid": self.createUid,
            "pushUid": self.pushUid
        }

        print("handle request from " + str(self.client_address))

        data = self.readline()
        print(data)

        switcher[data[0]](data[1:])


with DataServer((HOST, PORT), DataServerHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()



