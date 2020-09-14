
import socket
from os.path import getsize
import os

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

    def readfile(self, len):
        data = self.rfile.read(len)
        print(data)

    def sendFile(self, filepath):
        with open(filepath, "rb") as file:
            self.s.sendfile(file)

    def close(self):
        self.s.close()


def sendFile(localPath = "small_file.txt", remotePath="testfile.txt"):
    conn = Connection((HOST, PORT))
    size = getsize(localPath)
    print("file size", size)

    conn.write("pushPath", remotePath, size, "abcdef123456")
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

#sendFile("small_file.txt", "ale/file1")
#sendFile("small_file.txt", "ale/file2")
#sendFile("small_file.txt", "ale/file3")

def list(path=""):
    conn = Connection((HOST, PORT))
    conn.write("list", path)
    state, lines = conn.readline()
    for l in range(int(lines)):
        uid, path = conn.readline()
        print(uid, path)
    conn.close()

list("")