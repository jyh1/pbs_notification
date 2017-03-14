import subprocess
from collections import defaultdict
from socket import *
import threading
import time

serverPort = 12001

def checkPBS():
    p = subprocess.Popen("showq", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, _ = p.communicate()
    records = defaultdict(list)
    for l in stdout.split("\n"):
        tokens = l.split()
        if tokens != [] and tokens[0].isdigit() and (tokens[2] == "Running" or tokens[2] == "Idle" or tokens[2]=="BatchHold"):
            dic = dict()
            dic["name"] = tokens[0]
            dic["status"] = tokens[2]
            dic["time"] = tokens[4]
            dic["start"] = tokens[5:]
            records[tokens[1]].append(dic)
    return records

def getRecords():
    global records
    while True:
        records = checkPBS()
        time.sleep(60)



threading.Thread(target=getRecords, args=()).start()


def replyInfo(records, connectionSocket):
     data = connectionSocket.recv(1024)
     if data:
         connectionSocket.send(str(records[data.strip()]))
     connectionSocket.close()


serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.bind(('',serverPort))
serverSocket.listen(1)
print 'The server is ready to receive'
while 1:
     connectionSocket, _ = serverSocket.accept()
     connectionSocket.settimeout(60)
     threading.Thread(target=replyInfo, args=(records, connectionSocket)).start()
