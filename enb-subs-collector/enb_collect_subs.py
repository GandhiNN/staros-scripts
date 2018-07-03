#!/usr/local/bin/python3
#
# Collect subscriber data per ENB
# author : ngakan.gandhi@packet-systems.com
#

from paramiko import client
from pprint import pprint
import subprocess
import csv

class ssh:
    client = None

    def __init__(self, address, username, password):
        print('Connecting to server.')
        self.client = client.SSHClient()
        self.client.set_missing_host_key_policy(client.AutoAddPolicy())
        self.client.connect(address, username=username, password=password, look_for_keys=False)

    def sendCommand(self, command):
        if (self.client):
            stdin, stdout, stderr = self.client.exec_command(command)
            while not stdout.channel.exit_status_ready():
                # Only print data if there is data to read in the channel
                if stdout.channel.recv_ready():
                    alldata = stdout.channel.recv(1024)
                    prevdata = b"1"
                    while prevdata:
                        prevdata = stdout.channel.recv(1024)
                        alldata += prevdata

            PathFileName = '/home/edruser/scripts/test.csv'
            f = open(PathFileName, 'w')
            f.write(str(alldata, "utf8"))
            f.close()
        else:
            print('Connection is not opened.')

    def sendShowCommand(self, command):
        if (self.client):
            stdin, stdout, stderr = self.client.exec_command(command)
            while not stdout.channel.exit_status_ready():
                # Only print data if there is data to read in the channel
                if stdout.channel.recv_ready():
                    alldata = stdout.channel.recv(1024)
                    prevdata = b"1"
                    while prevdata:
                        prevdata = stdout.channel.recv(1024)
                        alldata += prevdata

            EnbFileName = '/home/edruser/scripts/log.csv'
            f = open(EnbFileName, 'a')
            f.write(str(alldata, "utf8"))
            f.close()
        else:
            print('Connection is not opened')

    def getEnbIp(self):
        PathLog = '/home/edruser/scripts/test.csv'
        globalEnbId = []
        with open(PathLog, 'r') as infile:
            enbIpPort = [line.split()[-2]
                     for line in infile
                     if 'mme-svc' in line]
            enbIp = [x.split(':')[0]
                     for x in enbIpPort]
        with open(PathLog, 'r') as infile:
            globalEnbId = [line.split()[2]
                           for line in infile
                           if 'mme-svc' in line]
        with open(PathLog, 'r') as infile:
            enbNameRaw = [line.split()[3]
                       for line in infile
                       if 'mme-svc' in line]
        enbName = ['NA' if x == 'mme-svc'
                   else x for x in enbNameRaw]

        return enbIp, globalEnbId, enbName

def csv_write(globalEnbIdList, enbIpList, enbNameList, enbActSub):
    dstLog = '/home/edruser/scripts/hasilujicoba.csv'
    with open(dstLog,'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['GlobalEnbId', 'EnbIpAddr', 'EnbName', 'EnbSubscriber'])
        count = 0
        while count < len(globalEnbIdList):
            writer.writerow([globalEnbIdList[count], enbIpList[count], enbNameList[count], enbActSub[count]])
            count += 1

connection = ssh('10.205.57.4', 'psiuser', 'Psi12345!')
connection.sendCommand('show mme-service enodeb-association full wf1 all')
enbIpList, globalEnbIdList, enbNameList = connection.getEnbIp()
bashCommand = "cat /dev/null > /home/edruser/scripts/log.csv"
subprocess.Popen(bashCommand, shell=True)
logFile = '/home/edruser/scripts/log.csv'

for enbIp in enbIpList:
    sendString = 'show subscribers data-rate enodeb-address ' + enbIp + '\n'
    print(sendString)
    f = open(logFile, 'a')
    f.write(str(sendString))
    f.close()
    connection.sendShowCommand(sendString)
with open(logFile, 'r') as original:
    data = original.read()
with open(logFile, 'w') as modified:
    modified.write('show subscribers data-rate enodeb-address ' + enbIpList[0] + '\n' + data)

with open('/home/edruser/scripts/log.csv', 'r') as ujifile:
    enbActSub = []
    for line in ujifile:
        if 'Total Subscribers' in line:
            enbActSub.append(line.split()[3])
        elif 'No subscribers' in line:
            enbActSub.append('NA')

# Write CSV File
csv_write(globalEnbIdList, enbIpList, enbNameList, enbActSub)

