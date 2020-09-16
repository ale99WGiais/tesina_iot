
from socketserver import *
import sqlite3
import os
import sys
import socket
import schedule
import psutil
import _thread
import hashlib
import time
from datetime import datetime

NAME = "dataserver10010"
HOST = "localhost"
PORT = 10010

#bandup and banddown in MB/s
performances = {"sent": 0, "recv": 0, "lastTime" : 0, "bandup" : 0, "banddown" : 0}

def getPerformance():
    res = psutil.net_io_counters()
    curtime = time.time()
    delta = curtime - performances["lastTime"]
    performances["lastTime"] = curtime
    performances["bandup"] = (res.bytes_sent - performances["sent"]) / delta / 1000000
    performances["banddown"] = (res.bytes_recv - performances["recv"]) / delta / 1000000
    performances["sent"] = res.bytes_sent
    performances["recv"] = res.bytes_recv
    #print(performances)

schedule.every(5).seconds.do(getPerformance)

def monitorPerformanceLoop(_):
    while True:
        schedule.run_pending()
        time.sleep(0.1)

_thread.start_new_thread(monitorPerformanceLoop, (None, ))

def hash_bytestr_iter(bytesiter, hasher, ashexstr=True):
    for block in bytesiter:
        hasher.update(block)
    return hasher.hexdigest() if ashexstr else hasher.digest()

def file_as_blockiter(afile, blocksize=65536):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)

def hashFile(path):
    return hash_bytestr_iter(file_as_blockiter(open(path, 'rb')), hashlib.sha1())

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

    def deleteUid(self, uid):
        self.cursor.execute("delete from object where uid = ?", (uid, ))
        self.connection.commit()

    def nodeStats(self):
        return self.cursor.execute("select * from stats").fetchone()

    def getStoredData(self):
        return self.cursor.execute("select uid, created, complete from object").fetchall()

    def reservedSpace(self):
        res = self.cursor.execute("select sum(size) from object").fetchone()[0]
        if res is None:
            res = 0
        return res

    def addObject(self, uid, local_path, size, checksum):
        complete = 0
        created = datetime.now()

        self.cursor.execute("insert into object(uid, local_path, size, complete, checksum, created) values (?, ?, ?, ?, ?, ?)",
                            (uid, local_path, size, complete, checksum, created))
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

    def sendFile(self, filepath, startIndex):
        if type(startIndex) != int:
            startIndex = int(startIndex)
        with open(filepath, "rb") as file:
            self.s.sendfile(file, startIndex)

    def close(self):
        self.s.close()

class DataServerHandler(StreamRequestHandler):
    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        print("write ", res)
        self.wfile.write(res.encode())

    def readline(self):
        return self.rfile.readline().strip().decode().split(";")

    def sendFile(self, filepath, startIndex):
        if type(startIndex) != int:
            startIndex = int(startIndex)
        with open(filepath, "rb") as file:
            self.connection.sendfile(file, startIndex)

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

    def status(self, args):
        global performances
        reservedCapacity = self.database.reservedSpace()
        totCapacity, downSpeed, upspeed = self.database.nodeStats()
        self.write("ok", reservedCapacity, totCapacity, downSpeed, performances["banddown"], upspeed, performances["bandup"])

    def deleteUid(self, args):
        uid, = args

        uid, localPath, size, complete, created, checksum = self.database.getObject(uid)

        if os.path.exists(localPath): os.remove(localPath)
        self.database.deleteUid(uid)

        self.write("ok")

    def pushUid(self, args):
        uid, = args

        objinfo = self.database.getObject(uid)

        if objinfo is None:
            self.write("ERR", "uid info not found")
            return False

        print(objinfo)

        uid, localpath, size, complete, created, checksum,  = objinfo

        if complete:
            self.write("err", "file complete")
            return False

        startIndex = 0
        if os.path.exists(localpath):
            startIndex = os.path.getsize(localpath)

        assert startIndex < size   #altrimenti sarebbe complete

        self.write("ok", startIndex)

        with open(localpath, "a+b") as out:
            self.readfile(size - int(startIndex), out)

        file_checksum = hashFile(localpath)

        print("checksum", checksum, "file_checksum", file_checksum)

        if file_checksum != checksum:
            os.remove(localpath)
            print("ERROR checksum do not match")
            self.write("err", "checksum do not match")
            return False

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

        uid, localPath, size, complete, created, checksum = res

        host, port = server.split(":")
        port = int(port)
        target = Connection((host, port))
        target.write("pushUid", uid)
        status, startIndex = target.readline()
        target.sendFile(localPath, startIndex)
        target.close()

        self.write("ok")

    def getUid(self, args):
        uid, startIndex = args
        startIndex = int(startIndex)

        res = self.database.getObject(uid)

        if res is None:
            self.write("err", "specified uid not present")
            return False

        print(res)

        uid, localPath, size, complete, created, checksum = res

        if not complete:
            self.write("err", "not complete")
            return False

        self.write("ok", size-startIndex)
        self.sendFile(localPath, startIndex)

    def getStoredData(self, args):
        data = self.database.getStoredData()
        self.write(len(data))
        for uid, created, complete in data:
            self.write(uid, created, complete)

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
            "test": self.test,
            "status": self.status,
            "deleteUid": self.deleteUid,
            "getStoredData": self.getStoredData
        }

        print("handle request from " + str(self.client_address))

        data = self.readline()
        print(data)

        switcher[data[0]](data[1:])


with DataServer((HOST, PORT), DataServerHandler) as server:
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()



