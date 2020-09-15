
import socket
from os.path import getsize
import os
import time

HOST = 'localhost'    # The remote host
PORT = 10000            # The same port as used by the server

os.chdir("code")
print(os.getcwd())

class Connection:
    def __init__(self, endpoint):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(endpoint)

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


    def sendFile(self, filepath):
        with open(filepath, "rb") as file:
            self.s.sendfile(file)

    def close(self):
        self.s.close()


def sendFile(localPath = "small_file.txt", remotePath="testfile.txt", priority=1):
    conn = Connection((HOST, PORT))
    size = getsize(localPath)
    print("file size", size)

    conn.write("pushPath", remotePath, size, "abcdef123456", priority)
    res = conn.readline()

    if res[0] != "ok":
        print("ERR", res)
        exit()

    state, uid, addr = res
    conn.close()

    addr = addr.split(":")
    addr = (addr[0], int(addr[1]))
    conn = Connection(addr)

    conn.write("pushUid", uid)

    conn.sendFile(localPath)
    print(conn.readline())
    conn.close()

def list(path=""):
    conn = Connection((HOST, PORT))
    conn.write("list", path)
    state, lines = conn.readline()
    for l in range(int(lines)):
        res = conn.readline()
        print(*res)
    conn.close()

def get(localPath = "testin.txt", remotePath = "ale/file1"):
    conn = Connection((HOST, PORT))
    conn.write("getPath", remotePath)
    res = conn.readline()
    conn.close()

    print(res)
    status, uid, addr = res
    ip, port = addr.split(":")
    port = int(port)

    conn = Connection((ip, port))
    conn.write("getUid", uid)
    res = conn.readline()
    print(res)
    status, size = res

    with open(localPath, "wb") as out:
        conn.readfile(size, out)

    conn.close()



def test():
    conn = Connection((HOST, PORT))
    conn.write("test")
    conn.close()

def deletePath(path):
    conn = Connection((HOST, PORT))
    conn.write("deletePath", path)
    print(conn.readline())
    conn.close()


def sendTestFiles():
    sendFile("small_file.txt", "ale/file1")
    sendFile("small_file.txt", "ale/file2")
    sendFile("small_file.txt", "ale/file3")

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
deletePath("ale/file2")