
import os
import signal
import subprocess

if not os.path.exists("testconfig"):
    os.mkdir("testconfig")
os.chdir("testconfig")
print(os.getcwd())

ds = (11000, 11010)

pids = []

for i in range(*ds):
    name = "dataserver" + str(i)
    with open(name + ".yaml", "w+") as fout:
        for x in ["name: " + name,
                  "workingDir: /home/ale/Dropbox/tesina iot/code/test/testdata/" + name,
                  "host: localhost",
                  "port: " + str(i),
                  "storage: 100000000",
                  "downspeed: 50",
                  "upspeed: 10",
                  "metaserver: localhost:10000"]:
            fout.write(x + "\n")

    # The os.setsid() is passed in the argument preexec_fn so
    # it's run after the fork() and before  exec() to run the shell.
    cmd = '/home/ale/miniconda3/envs/tesina_iot/bin/python "/home/ale/Dropbox/tesina iot/code/dataserver/dataserver.py" "' + os.getcwd() + "/" + name + '.yaml"'
    print(cmd)
    pro = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

    pids.append(pro.pid)

print(pids)

with open("metaserver.yaml", "w+") as fout:
    fout.write("dataservers:\n")

    for i in range(*ds):
        fout.write("  - localhost:" + str(i) + "\n")

cmd = '/home/ale/miniconda3/envs/tesina_iot/bin/python "/home/ale/Dropbox/tesina iot/code/metaserver/metaserver.py" "' + os.getcwd() + '/metaserver.yaml"'
print(cmd)
pro = subprocess.Popen(cmd,
                       shell=True, preexec_fn=os.setsid)
pids.append(pro.pid)


input()

print("exit")
for pid in pids:
    os.killpg(pid, signal.SIGTERM)
