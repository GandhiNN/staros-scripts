#!/usr/local/bin/python3

import time
import paramiko
import os
import json
from itertools import repeat
from multiprocessing import Pool

# Load the node ssh profile
def getNodeLogin(node_config_file):
    with open(node_config_file) as node_confile:
        node_config = json.load(node_confile)
        node = []
        ip_addr = []
        user = []
        password = []
        # Populate lists
        for item in node_config:
            node.append(item['nodename'])
            ip_addr.append(item['ip_address'])
            user.append(item['user'])
            password.append(item['password'])
        return node, ip_addr, user, password

def delSsd(node, host, username, password):
    print("Deleting {0} Auto-Generated SSD File...".format(node))
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, password=password)
    channel = client.invoke_shell()
    resp = channel.recv(100000)
    channel.send('cli test-commands password boxer\n')
    time.sleep(2)
    alldata = ''
    if channel.recv_ready():
        resp = channel.recv(100000)
        alldata = resp
    channel.send('debug bang bash\n')
    time.sleep(2)
    if channel.recv_ready():
        resp = channel.recv(100000)
        alldata += resp
    channel.send('cd /flash/sftp/ssd\n')
    time.sleep(2)
    if channel.recv_ready():
        resp = channel.recv(100000)
        alldata += resp
    channel.send('rm -f {}*\n'.format(node))
    time.sleep(5)
    if channel.recv_ready():
        resp = channel.recv(100000)
        alldata += resp
    client.close()
    print("Finished Auto Generated SSD Deletion for {0} !".format(node))

# MAIN
def main():
    node_config_file = '/home/backup/scripts/node.json'
    node, ip_addr, user, password = getNodeLogin(node_config_file)
    n = len(node)
    timenow = time.strftime('%Y-%m-%d-%H-%M-%S')
    print('====' * 10)
    print(timenow)
    print('====' * 10)
    delete_ssd = Pool(n).starmap(delSsd, zip(node, ip_addr, user, password))

if __name__ == '__main__':
    main()

