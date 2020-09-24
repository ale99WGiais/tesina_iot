

from socketserver import *
from cassandra.cluster import Cluster
from uuid import uuid4, UUID
from datetime import datetime
import socket
import random
import schedule
import psutil
import _thread
import time
import hashlib
import dateutil.parser
import yaml
import logging
import sys

#logging.basicConfig(level=logging.NOTSET, format='(%(threadName)-9s) %(message)s',)

config = None
if len(sys.argv) > 1:
    configFile = sys.argv[1]
    print("load config", configFile)
    config = yaml.full_load(open(configFile, "r"))
    print("config", config)

#res = session.execute("select * from system.local;").all()
#print(res)


class Connection:
    def __init__(self, endpoint):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(addrFromString(endpoint))

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

def addrFromString(addr):
    ip, port = addr.split(":")
    return (ip, int(port))


class Database:
    def __init__(self):
        self.cluster = Cluster()

        self.session = self.cluster.connect("metaserver")
        self.statements = {}
        self.statements["addObject"] = self.session.prepare(
            "insert into object(uid, path, created, owner, size, priority, checksum) values (?, ?, ?, ?, ?, ?, ?)")
        self.statements["addStoredObject"] = self.session.prepare(
            "insert into stored_object(uid, server, created, complete) values (?, ?, ?, ?)")
        self.statements["listFilter"] = self.session.prepare("select * from object where path like ?")
        self.statements["list"] = self.session.prepare("select * from object")

        #print(self.session)

    def addDataServer(self, addr):
        self.session.execute("insert into dataserver(server, online) values (%s, false)", (addr, ))

    def addObject(self, uid, path, owner, size, priority, checksum):
        if type(uid) == "str":
            uid = UUID(uid)
        created = datetime.now()
        size = int(size)
        priority = int(priority)
        self.session.execute(self.statements["addObject"], (uid, path, created, owner, size, priority, checksum))
        return uid

    def addStoredObject(self, uid, server, complete=False, created=None):
        uid = str(uid)
        uid = UUID(uid)
        #uid = UUID(uid)
        if created is None: created = datetime.now()
        self.session.execute(self.statements["addStoredObject"], (uid, server, created, complete))
        return id

    def lockPath(self, path, user="root"):
        return self.session.execute("insert into object_lock(path, user) values (%s, %s) if not exists",
                                    (path, user)).one().applied

    def unlockPath(self, path):
        self.session.execute("delete from object_lock where path = %s", (path, ))

    def getPathLock(self, path):
        r = self.session.execute("select * from object_lock where path = %s", (path, )).one()
        if r is None:
            return False, ""
        else:
            return True, r.user

    def getObjectByUid(self, uid):
        uid = str(uid)
        uid = UUID(uid)
        return self.session.execute("select * from object where uid = %s", (uid, )).one()

    def list(self, path):
        if path == "":
            return self.session.execute(self.statements["list"]).all()
        else:
            path = path + "%"
            return self.session.execute(self.statements["listFilter"], (path, )).all()

    def getDataServers(self, online=True):
        if online:
            return self.session.execute("select * from dataserver where online=true allow filtering").all()
        return self.session.execute("select * from dataserver").all()

    def updateDataServerStatus(self, addr, online, remaining_capacity=0, capacity=0, available_down=.0, available_up=.0):
        self.session.execute("update dataserver "
                             "set capacity=%s, remaining_capacity=%s, available_down=%s, "
                             "available_up=%s, online=%s where server=%s",
                             (capacity, remaining_capacity,
                              available_down, available_up, online, addr))
        self.session.execute("insert into performance_log(server, time, capacity, remaining_capacity, available_down,"
                             "available_up, online) values (%s, %s, %s, %s, %s, %s, %s)",
                             (addr, datetime.now(), capacity, remaining_capacity,
                              available_up, available_down, online))

    def removeStoredObject(self, uid, server):
        uid = str(uid)
        uid = UUID(uid)
        self.session.execute("delete from stored_object where uid = %s and server = %s", (uid, server))

    def removeObject(self, uid):
        uid = UUID(uid)
        self.session.execute("delete from object where uid = %s", (uid, ))

    def getServersForUid(self, uid, complete=True, online=True):
        uid = str(uid)
        uid = UUID(uid)
        if complete:
            res = self.session.execute(
                "select server from stored_object where uid = %s and complete = true allow filtering", (uid, )).all()
        else:
            res = self.session.execute(
                "select server from stored_object where uid = %s allow filtering", (uid, )).all()
        if online:
            def f(srv):
                return self.session.execute("select online from dataserver where server = %s", (srv.server, )).one().online
            res = list(filter(f, res))
        print("getServersForUid", uid, res)
        return res

    def isServerOnline(self, server):
        return self.session.execute("select online from dataserver where server = %s", (server, )).one().online

    def setComplete(self, uid, server):
        uid = UUID(uid)
        self.session.execute("update stored_object set complete = true where uid = %s and server = %s", (uid, server))

    def markDeleted(self, path):
        res = self.getUidForPath(path)
        timestamp = datetime.now()
        if res is not None:
            self.session.execute("update object set deleted = %s where uid = %s and created = %s",
                                 (timestamp, res.uid, res.created))

    def markUidDeleted(self, uid, created):
        timestamp = datetime.now()
        self.session.execute("update object set deleted = %s where uid = %s and created = %s",
                             (timestamp, uid, created))

    def getUidForPath(self, path):
        print("path", path)
        path = str(path)
        return self.session.execute("select * from pathToObject where path = %s", (path, )).one()

    def getUidsForPath(self, path, noDeleted=False):
        print("path", path)
        path = str(path)
        return self.session.execute("select * from object where path like %s", (path, )).all()

    def getUidsForServer(self, server):
        return self.session.execute("select uid from stored_object where server = %s allow filtering", (server, ))

    def addPendingUid(self, uid):
        uid = str(uid)
        uid = UUID(uid)
        self.session.execute("insert into pending_object(uid, enabled) values (%s, True)", (uid, ))

    def updatePriority(self, uid, created, priority):
        uid = str(uid)
        uid = UUID(uid)
        self.session.execute("update object set priority=%s where uid=%s and created=%s", (priority, uid, created))

    def getPendingUids(self, onlyEnabled=True):
        if onlyEnabled:
            return self.session.execute("select uid from pending_object where enabled=True allow filtering").all()
        return self.session.execute("select uid from pending_object").all()

    def disablePendingUid(self, uid):
        uid = str(uid)
        uid = UUID(uid)
        self.session.execute("update pending_object set enabled=False where uid = %s", (uid, ))

    def removePendingUid(self, uid):
        uid = str(uid)
        uid = UUID(uid)
        self.session.execute("delete from pending_object where uid = %s", (uid, ))


if config is not None:
    database = Database()
    for dataserver in config["dataservers"]:
        database.addDataServer(dataserver)
        print("add dataserver", dataserver)

def processPendingUids():
    database = Database()

    for elem in database.getPendingUids():
        processPendingUid(database, elem.uid)

def processPendingUid(database, uid):
    serversContaining = {x.server for x in database.getServersForUid(uid)}
    numCopies = len(serversContaining)

    print("serversContaining", serversContaining)
    availableServers = [x.server for x in database.getDataServers() if x.server not in serversContaining]

    print("availableServers", availableServers)

    res = database.getObjectByUid(uid)
    print(res)
    priority = res.priority
    size = res.size
    checksum = res.checksum
    print("priority", priority)

    serversContaining = list(serversContaining)

    if numCopies == priority:
        database.removePendingUid(uid)
        return

    if numCopies > priority:
        random.shuffle(serversContaining)
        target = serversContaining[0]

        print("remove", uid, "target", target)

        conn = Connection(target)
        conn.write("deleteUid", uid)
        res = conn.readline()
        print(res)
        conn.close()

        database.removeStoredObject(uid, target)
        return

    if numCopies < priority:

        if len(serversContaining) == 0:
            database.disablePendingUid(uid)
            return

        if len(availableServers) > 0:
            random.shuffle(availableServers)
            random.shuffle(serversContaining)

            try:
                source = serversContaining[0]
                target = availableServers[0]

                database.addStoredObject(uid, target)

                conn = Connection(target)
                conn.write("createUid", uid, size, checksum)
                print(conn.readline())
                conn.close()

                conn = Connection(source)
                conn.write("transfer", uid, target)
                print(conn.readline())
                conn.close()

                #database.removePendingUid(uid)
            except:
                print("ERROR")
        else:
            database.disablePendingUid(uid)

def onDataServerConnect(database, addr):
    print("server", addr, "connected")

    for elem in database.getUidsForServer(addr):
        database.addPendingUid(elem.uid)

    for elem in database.getPendingUids(onlyEnabled=False):
        database.addPendingUid(elem.uid)

def onDataServerDisconnect(database, addr):
    if database.isServerOnline(addr):
        database.updateDataServerStatus(addr, False)

        for elem in database.getUidsForServer(addr):
            database.addPendingUid(elem.uid)

        print("server", addr, "disconnected")


def checkDataServerStatus(database, addr):
    try:
        wasOnline = database.isServerOnline(addr)

        # print("monitor", addr)
        conn = Connection(addr)
        conn.write("status")
        res = conn.readline()
        conn.close()
        # print(res)
        status, reservedCapacity, totCapacity, downSpeed, bandDown, upspeed, bandUp = res
        reservedCapacity = int(reservedCapacity)
        totCapacity = int(totCapacity)
        downSpeed = float(downSpeed)
        bandDown = float(bandDown)
        upspeed = float(upspeed)
        bandUp = float(bandUp)
        database.updateDataServerStatus(addr, True, totCapacity - reservedCapacity, totCapacity,
                                        downSpeed - bandDown, upspeed - bandUp)

        if not wasOnline:
            onDataServerConnect(database, addr)
    except ConnectionRefusedError as err:
        print(err)
        onDataServerDisconnect(database, addr)


def monitorDataServers():
    database = Database()
    for server in database.getDataServers(online=False):
        checkDataServerStatus(database, server.server)


schedule.every(4).seconds.do(monitorDataServers)
schedule.every(5).seconds.do(processPendingUids)
schedule.run_all()

def repeatedActions(_):
    while True:
        schedule.run_pending()
        time.sleep(0.1)

_thread.start_new_thread(repeatedActions, (None,))

class MetaServer(ThreadingTCPServer):
    def server_activate(self):
        ThreadingTCPServer.server_activate(self)
        print("starting metaserver at " + str(self.server_address))

class MetaServerHandler(StreamRequestHandler):
    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        print("write", res)
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

    def _getServerForUid(self, uid):
        res = self.database.getServersForUid(uid)
        print("servers", res)

        if len(res) == 0:
            self.write("err", "no copies available")
            return False

        random.shuffle(res)

        addr = res[0].server
        return addr

    def getPath(self, args):
        path, = args

        res = self.database.getUidForPath(path)

        if not res:
            self.write("err", "path not found")
            return False

        if res.deleted is not None:
            self.write("err", "path deleted")
            return False

        uid = res.uid
        print("uid", uid)

        addr = self._getServerForUid(uid)

        self.write("ok", uid, addr)

    def deletePath(self, args):
        path, = args

        res = self.database.getUidsForPath(path)

        for elem in res:
            uid = elem.uid
            print("delete", uid)
            if elem.deleted is None:
                self.database.markUidDeleted(uid, elem.created)

        self.write("ok")

    def permanentlyDeletePath(self, args):
        path, = args

        res = self.database.getUidsForPath(path)

        for elem in res:
            uid = elem.uid
            print("permanentlyDelete", uid)

            if elem.deleted is None:
                self.database.markUidDeleted(uid, elem.created)
            self.database.updatePriority(uid, elem.created, 0)
            self.database.addPendingUid(uid)

        self.write("ok")

    def updatePriorityForPath(self, args):
        path, priority = args
        priority = int(priority)

        res = self.database.getUidForPath(path)

        if res is None:
            self.write("err", "path " + path + " not found")
            return False

        uid = res.uid
        print(uid)

        self.database.updatePriority(uid, res.created, priority)
        self.database.addPendingUid(uid)

        self.write("ok")

    def updatePriorityForUid(self, args):
        uid, priority = args
        priority = int(priority)

        res = self.getUid(uid)

        self.database.updatePriority(uid, res.created, priority)
        self.database.addPendingUid(uid)

        self.write("ok")

    def pushPath(self, args):
        path, size, checksum, priority, user = args
        uid = uuid4()

        lock, lockUser = self.database.getPathLock(path)
        if lock and lockUser != user:
            self.write("err", "path locked by " + lockUser)
            return False

        dataservers = self.database.getDataServers()

        if len(dataservers) == 0:
            self.write("err", "no data servers available")
            return False

        random.shuffle(dataservers)

        size = int(size)
        target = None
        for target in dataservers:
            print("option server", target.server, "rem capacity", target.remaining_capacity, "file size", size)
            if target.remaining_capacity > size:
                break
            else:
                target = None

        if target is None:
            self.write("err", "no dataserver with sufficient capacity")
            return False

        addr = target.server

        dataServer = Connection(addr)
        dataServer.write("createUid", uid, size, checksum)
        response = dataServer.readline()
        dataServer.close()

        if response[0] != "ok":
            print("ERROR", response[0])
            self.write("err", response)
            return False

        self.database.markDeleted(path)
        self.database.addObject(uid, path, "root", size, priority, checksum)
        self.database.addStoredObject(uid, addr)

        self.write("ok", uid, addr)

    def pushComplete(self, args):
        uid, addr = args

        self.database.setComplete(uid, addr)
        self.database.addPendingUid(uid)

        self.write("ok")

    def list(self, args):
        path, = args
        res = self.database.list(path)
        print(res)
        self.write("ok", len(res))
        for line in res:
            self.write(*line)

    def getUid(self, args):
        uid, = args

        addr = self._getServerForUid(uid)

        self.write("ok", addr)

    def addDataServer(self, args):
        addr, = args

        self.database.addDataServer(addr)

        checkDataServerStatus(self.database, addr)

        conn = Connection(addr)
        conn.write("getStoredData")
        len, = conn.readline()

        for i in range(int(len)):
            uid, created, complete = conn.readline()
            created = dateutil.parser.parse(created)
            complete = complete == "1"
            self.database.addStoredObject(uid, addr, complete, created)

        self.write("ok")

    def lockPath(self, args):
        path, user = args
        res = self.database.lockPath(path, user)
        self.write(res)

    def getPathLock(self, args):
        path, = args
        lock, user = self.database.getPathLock(path)
        self.write(lock, user)

    def unlockPath(self, args):
        path, = args
        self.database.unlockPath(path)
        self.write("ok")

    def _deleteUid(self, uid, server):
        self.database.removeStoredObject(uid, server)

        conn = Connection(server)
        conn.write("deleteUid", uid)
        res = conn.readline()
        print(res)

    def test(self, args):
        print("test")

        print(self.database.lockPath("lock1"))
        print(self.database.lockPath("lock1"))
        print(self.database.isPathLocked("lock1"))
        print(self.database.unlockPath("lock1"))
        print(self.database.isPathLocked("lock1"))

        pass

    def handle(self):
        self.database = Database()

        switcher = {
            "getPath": self.getPath,
            "pushPath": self.pushPath,
            "list": self.list,
            "pushComplete": self.pushComplete,
            "test": self.test,
            "deletePath": self.deletePath,
            "addDataServer": self.addDataServer,
            "getUid": self.getUid,
            "lockPath": self.lockPath,
            "getPathLock": self.getPathLock,
            "unlockPath": self.unlockPath,
            "updatePriorityForPath": self.updatePriorityForPath,
            "updatePriorityForUid": self.updatePriorityForUid,
            "permanentlyDeletePath": self.permanentlyDeletePath
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