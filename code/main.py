
import socket
from os.path import getsize
import os
import time
import hashlib

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

def addrFromString(addr):
    ip, port = addr.split(":")
    return (ip, int(port))

class Connection:
    def __init__(self, endpoint):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(addrFromString(endpoint))

        self.wfile = self.s.makefile('wb', 0)
        self.rfile = self.s.makefile('rb', -1)

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
            #print("read", chunk, "bytes")
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


def sendFile(localPath = "small_file.txt", remotePath="testfile.txt", priority=1):
    conn = Connection(METASERVER)
    size = getsize(localPath)
    print("file size", size)

    checksum = hashFile(localPath)

    conn.write("pushPath", remotePath, size, checksum, priority)
    res = conn.readline()

    if res[0] != "ok":
        print("ERR", res)
        return False

    state, uid, addr = res
    conn.close()

    conn = Connection(addr)

    conn.write("pushUid", uid)

    res = conn.readline()
    if res[0] != 'ok':
        print(res)
        return False

    status, startIndex = res

    print("send starting from", int(startIndex))

    conn.sendFile(localPath, startIndex)
    print(conn.readline())
    conn.close()

def list(path=""):
    conn = Connection(METASERVER)
    conn.write("list", path)
    state, lines = conn.readline()
    for l in range(int(lines)):
        res = conn.readline()
        print(*res)
    conn.close()

def get(localPath = "testin.txt", remotePath = "ale/file1", newFile=True):
    if newFile and os.path.exists(localPath):
        os.remove(localPath)

    conn = Connection(METASERVER)
    conn.write("getPath", remotePath)
    res = conn.readline()
    conn.close()

    print(res)

    if res[0] != 'ok':
        print(res)
        return False

    status, uid, addr = res

    startIndex = 0
    if os.path.exists(localPath):
        startIndex = os.path.getsize(localPath)

    conn = Connection(addr)
    conn.write("getUid", uid, startIndex)
    res = conn.readline()
    print(res)
    status, size = res

    with open(localPath, "a+b") as out:
        conn.readfile(size, out)

    conn.close()



def test():
    conn = Connection(METASERVER)
    conn.write("test")
    conn.close()

def deletePath(path):
    conn = Connection(METASERVER)
    conn.write("deletePath", path)
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

def getUid(uid):
    conn = Connection(METASERVER)
    conn.write("getUid", uid)
    print(conn.readline())
    conn.close()



sendFile(priority=2)



addDataServer("localhost:10010")
addDataServer("localhost:10011")
addDataServer("localhost:10012")

sendTestFiles()
list()
get(remotePath="ale/file3")
#test()

sendFile(remotePath="testPriority2", priority=2)
time.sleep(3)
get(remotePath="testPriority2")
get(remotePath="testPriority2")
get(remotePath="testPriority2")
get(remotePath="testPriority2")

