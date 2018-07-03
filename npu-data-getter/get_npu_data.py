#!/usr/local/bin/python3
#
# author : ngakan.gandhi@packet-systems.com
# 5 Jun 2017
#

import subprocess
import pexpect
import time
import json
from itertools import repeat
from multiprocessing.dummy import Pool

def load_node_config(node_conf_file):
    with open(node_conf_file) as nodefile:
        loaded = json.load(nodefile)
        node = []
        nodetype = []
        ip_address = []
        user = []
        password = []
        # Populate list
        for item in loaded:
            if ("GGCBT17") in item['nodename']:
                node.append(item['nodename'])
                nodetype.append(item['nodetype'])
                ip_address.append(item['ip_address'])
                user.append(item['user'])
                password.append(item['password'])
            if ("GGCBT18") in item['nodename']:
                node.append(item['nodename'])
                nodetype.append(item['nodetype'])
                ip_address.append(item['ip_address'])
                user.append(item['user'])
                password.append(item['password'])
        return node, ip_address, user, password

def print_to_file(text, output_file):
    with open(output_file, 'a') as OF:
        OF.write(text + "\n")

def node_connect_log_queries(node, ipaddr, user, password, timenow):
    base_dir = "/home/backup/npu_history/log/"
    base_log = base_dir + node + "/" + node + "-npu-history-" + timenow + ".log"
    with open(base_log, 'w') as basefile:
        node_expect = node + "#"
        ssh_command = "ssh " + user + "@" + ipaddr
        enter_hidden_command = "cli test-commands password boxer"
        npu_util_table_query = "show npu utilization table"
        card_hardware_query = "show card hardware"
        npumgr_util_query = "show npumgr utilization information"
        child = pexpect.spawnu(ssh_command)
        child.logfile = basefile
        child.expect("password:")
        child.sendline(password)
        child.expect(node_expect)
        child.sendline(enter_hidden_command)
        child.expect(node_expect)
        child.sendline(npu_util_table_query)
        child.expect(node_expect)
        child.sendline(card_hardware_query)
        child.expect(node_expect)
        child.sendline(npumgr_util_query)
        child.expect(node_expect)
        child.sendline("exit")

def main():
    node_conf_file = "/home/backup/scripts/node.json"
    timenow = time.strftime("%d-%m-%Y-%H-%M") + "-00"
    node, ip_addr, user, password = load_node_config(node_conf_file)
    i = len(node)
    get_npu = Pool(i).starmap(node_connect_log_queries, zip(node, ip_addr, 
                              user, password, repeat(timenow)))

if __name__ == "__main__":
    main()
