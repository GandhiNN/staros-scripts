#!/usr/local/bin/python3

import time
import paramiko
import os
import json
import csv
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
            if 'SGSNMME' in item['nodetype']:
                node.append(item['nodename'])
                ip_addr.append(item['ip_address'])
                user.append(item['user'])
                password.append(item['password'])
        return node, ip_addr, user, password

def nodeLogin(node, cur_time, address, username, password):
    output_file = '/home/backup/saau/raw/' + node.upper() + '/' \
        + node.upper() + '-' + cur_time + '-saau.raw'
    log_file = open(output_file, 'wb')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(address, username=username, password=password)
    # Paramiko interactive shell mode
    channel = client.invoke_shell()
    resp = channel.recv(450000)
    # SAU-SAAU 4G
    channel.send('show mme-service statistics\n')
    time.sleep(90)
    # 'alldata' is a container for all commands output
    alldata = ''
    # 'recv_ready' avoids blocking on reading data that might not ever arrive
    if channel.recv_ready():
        resp = channel.recv(450000)
        alldata = resp
    # SAU-SAAU 2G & 3G
    channel.send('show gmm-sm statistics verbose\n')
    time.sleep(90)
    # 'recv_ready' avoids blocking on reading data that might not ever arrive
    if channel.recv_ready():
        resp = channel.recv(450000)
        alldata += resp
    log_file.write(alldata)
    client.close()
    log_file.close()

def createCsv(node, cur_time, t_stamp, cur_day):
    rawfile_path = '/home/backup/saau/raw/' + node.upper() + '/' \
        + node.upper() + '-' + cur_time + '-saau.raw'
    csvfile_path = '/home/backup/saau/parsed/' + node.upper() + '/'\
        + node.upper() + '-' + cur_day + '-saau.csv'
    # Dict definition for SAU/SAAU
    sau2g = ''
    sau3g = ''
    sau4g = ''
    saau2g = ''
    saau3g = ''
    saau4g = ''
    with open(rawfile_path, 'r') as infile:
        for line in infile:
            if '3G Attached' and '2G Attached' in line:
                sau2g = str(line.strip().split()[5])
                sau3g = str(line.strip().split()[2])
            elif '3G Activated' and '2G Activated' in line:
                saau2g = str(line.strip().split()[5])
                saau3g = str(line.strip().split()[2])
            elif 'Total Subscribers' in line:
                line = next(infile)
                sau4g = str(line.strip().split()[2])
                saau4g = str(line.strip().split()[5])
    sau_data = dict(timestamp=t_stamp, sau2g=sau2g, sau3g=sau3g, sau4g=sau4g, \
            saau2g=saau2g, saau3g=saau3g, saau4g=saau4g)
    header = list(sau_data.keys())
    value = list(sau_data.values())
    with open(csvfile_path, 'a+') as outfile:
        # Ensure that we're at the start of the file
        outfile.seek(0)
        # Get the first char of the file
        first_char = outfile.read(1)
        writer = csv.writer(outfile)
        if not first_char:
            writer.writerow(header)
            writer.writerow(value)
        else:
            writer.writerow(value)

def main():
    cur_time = time.strftime('%Y-%m-%d-%H-%M-00')
    t_stamp = time.strftime('%Y%m%d-%H%M00')
    cur_day = time.strftime('%Y-%m-%d')
    node_config_file = '/home/backup/scripts/node.json'
    node, ip_addr, user, password = getNodeLogin(node_config_file)
    n = len(node)
    get_raw_log = Pool(n).starmap(nodeLogin, zip(node, repeat(cur_time), ip_addr, user, password))
    get_parsed_log = Pool(n).starmap(createCsv, zip(node, repeat(cur_time), repeat(t_stamp), repeat(cur_day)))

if __name__ == '__main__':
    main()
