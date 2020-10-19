
import socket
from os.path import getsize
import os
import time
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='(%(threadName)-9s) %(message)s',)

METASERVER = "localhost:10000"

os.chdir("code")
print(os.getcwd())


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

def CustomConnection(cls):
    def addrFromString(self, addr):
        ip, port = addr.split(":")
        return (ip, int(port))

    def write(self, *args):
        res = ";".join([str(i) for i in args]).strip() + "\n"
        logging.info("write %s", res)
        self.wfile.write(res.encode())

    def readline(self):
        line = self.rfile.readline().strip().decode().split(";")
        logging.info("readline %s", line)
        return line

    def readFile(self, size, outFile):
        size = int(size)
        pos = 0
        while pos != size:
            chunk = min(1024, size - pos)
            # print("read", chunk, "bytes")
            data = self.rfile.read(chunk)
            outFile.write(data)
            pos += len(data)

    def writeFile(self, filepath, startIndex):
        if type(startIndex) != int:
            startIndex = int(startIndex)
        with open(filepath, "rb") as file:
            self.sendfile(file, startIndex)

    setattr(cls, "writeFile", writeFile)
    setattr(cls, "addrFromString", addrFromString)
    setattr(cls, "readFile", readFile)
    setattr(cls, "write", write)
    setattr(cls, "readline", readline)

    return cls

@CustomConnection
class Connection(socket.socket):
    def __init__(self, endpoint):
        super(Connection, self).__init__(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(self.addrFromString(endpoint))
        self.wfile = self.makefile('wb', 0)
        self.rfile = self.makefile('rb', -1)


def sendFile(localPath = "small_file.txt", remotePath="testfile.txt", priority=1, user="default"):
    with Connection(METASERVER) as conn:
        size = getsize(localPath)
        print("file size", size)

        checksum = hashFile(localPath)

        conn.write("pushPath", remotePath, size, checksum, priority, user)
        res = conn.readline()

        if res[0] != "ok":
            print("ERR", res)
            return False

        state, uid, addr = res

    with Connection(addr) as conn:
        conn.write("pushUid", uid)

        res = conn.readline()
        if res[0] != 'ok':
            print(res)
            return False

        status, startIndex = res

        print("send starting from", int(startIndex))

        conn.writeFile(localPath, startIndex)
        print(conn.readline())


def list(path="%"):
    conn = Connection(METASERVER)
    conn.write("list", path)
    state, lines = conn.readline()
    for l in range(int(lines)):
        res = conn.readline()
        print(*res)
    conn.close()

def lockPath(path, user="default"):
    conn = Connection(METASERVER)
    conn.write("lockPath", path, user)
    res, = conn.readline()
    print("lockPath", path, "lock", res, "by", user)
    return res

def getPathLock(path):
    conn = Connection(METASERVER)
    conn.write("getPathLock", path)
    res, user = conn.readline()
    print("getPathLock", path, "lock", res, "by", user)
    return res

def unlockPath(path):
    conn = Connection(METASERVER)
    conn.write("unlockPath", path)
    res, = conn.readline()
    print("unlockPath", path, res)
    return res

def get(localPath = "testin.txt", remotePath = "ale/file1", newFile=True, user="default"):
    if newFile and os.path.exists(localPath):
        os.remove(localPath)

    with Connection(METASERVER) as conn:
        conn.write("getPath", remotePath, user)
        res = conn.readline()

    print(res)

    if res[0] != 'ok':
        print("ERR", res)
        return False

    status, uid, addr, checksum = res

    startIndex = 0
    if os.path.exists(localPath):
        startIndex = os.path.getsize(localPath)

    with Connection(addr) as conn:
        conn.write("getUid", uid, startIndex)
        res = conn.readline()
        print(res)
        status, size = res

        with open(localPath, "a+b") as out:
            conn.readFile(size, out)

    if checksum != hashFile(localPath):
        logging.error("HASH don't match")
        return False

    return True

def test():
    conn = Connection(METASERVER)
    conn.write("test")
    conn.write("bella", "ziooo")
    print(conn.readline())
    conn.close()

def deletePath(path, user="default"):
    conn = Connection(METASERVER)
    conn.write("deletePath", path, user)
    print(conn.readline())
    conn.close()


def sendTestFiles():
    sendFile("small_file.txt", "ale/file1")
    sendFile("small_file.txt", "ale/file2")
    sendFile("small_file.txt", "ale/file3")

def addDataServer(addr):
    conn = Connection(METASERVER)
    conn.write("addDataServer", addr)
    conn.close()

def getUid(uid, localPath = "testin.txt", newFile=True, user="default"):
    if newFile and os.path.exists(localPath):
        os.remove(localPath)

    with Connection(METASERVER) as conn:
        conn.write("getUid", uid, user)
        res = conn.readline()

    print(res)

    if res[0] != 'ok':
        print("ERR", res)
        return False

    status, addr, checksum = res

    startIndex = 0
    if os.path.exists(localPath):
        startIndex = os.path.getsize(localPath)

    with Connection(addr) as conn:
        conn.write("getUid", uid, startIndex)
        res = conn.readline()
        print(res)
        status, size = res

        with open(localPath, "a+b") as out:
            conn.readFile(size, out)

    if checksum != hashFile(localPath):
        logging.error("HASH don't match")
        return False

    return True

def permanentlyDeletePath(path, user="default"):
    conn = Connection(METASERVER)
    conn.write("permanentlyDeletePath", path, user)
    print(conn.readline())
    conn.close()

def updatePriorityForPath(path, priority, user="default"):
    conn = Connection(METASERVER)
    conn.write("updatePriorityForPath", path, priority, user)
    print(conn.readline())
    conn.close()



from chrono import Timer
from concurrent.futures import *

ex = ThreadPoolExecutor(max_workers=50)

with Timer() as timer:
    def f(i):
        sendFile("small_file.txt", "test/" + str(i))
    ex.map(f, range(100))
    ex.shutdown()


print("elapsed", timer.elapsed)

exit(0)


sendFile("small_file.txt", "ale/file1")
sendFile("small_file.txt", "ale/file2")
sendFile("small_file.txt", "ale/file3")

get(remotePath="ale/file1")

input()

sendFile(remotePath="ale/filePriority2", priority=2)

input()

list()

input()

get(remotePath="ale/filePriority2", localPath="testin.txt")

input()

deletePath("ale/file3")

input()

get(remotePath="ale/file3")

input()

permanentlyDeletePath("ale/%")
