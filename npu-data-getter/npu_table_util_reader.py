#!/usr/local/bin/python3
#
# author : ngakan.gandhi@packet-systems.com
# 15 Jun 2017
# npu table parser and stats collection

import fileinput
import subprocess
import pexpect
import pprint
import time
import json
import os
import pandas as pd
import subprocess
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

def dead(reason):
    print(reason)

def line_prepender(node, npu_util_log_temp, npu_util_log, line_to_prepend):
    temp_file = "/home/backup/npu_history/csv/temp/" + node + "-newfile.txt"
    with open(npu_util_log_temp, 'r') as f:
        with open(temp_file, 'w') as f2:
            f2.write(line_to_prepend + '\n')
            f2.write(f.read())
    os.rename(temp_file, npu_util_log)

def node_connect_log_queries(node, ipaddr, user, password, timenow, daynow):
    base_dir = "/home/backup/npu_history/csv/"
    base_log = base_dir + node + "-npu-table-" + timenow + ".log"
    npu_util_log = base_dir + node + "/" + node + "-npu-stats-" + daynow + ".csv"
    #npu_util_log_temp = base_dir + node + "/" + node + "-npu-stats-temp-" + daynow + ".csv"
    npu_util_log_temp = "/home/backup/npu_history/csv/temp/" + node \
                        + "-npu-stats-temp-" + daynow + ".csv"
    with open(base_log, 'w') as basefile:
        node_expect = node + "#"
        ssh_command = "ssh " + user + "@" + ipaddr
        npu_util_table_query = "show npu utilization table"
        child = pexpect.spawnu(ssh_command)
        child.logfile = basefile
        child.expect("password:")
        child.sendline(password)
        child.expect(node_expect)
        child.sendline(npu_util_table_query)
        child.expect(node_expect)
        child.sendline("exit")
    header = get_core_load(base_log, npu_util_log_temp, timenow, daynow, node)
    header_str = ','.join(header)
    line_prepender(node, npu_util_log_temp, npu_util_log, header_str)
    os.remove(base_log)

def get_core_load(base_log, npu_util_log_temp, timenow, daynow, node):
    with open(base_log, 'r') as parse_log:
        card_core_name = []
        card_core_load = []
        # Hacky hack to add timestamp in each record
        card_core_name.append('timestamp')
        card_core_load.append(timenow)
        for line in parse_log:
            if "/0/" in line:
                card_core_name.append(line.strip().split()[0])
                card_core_load.append(line.strip().split()[1].replace("%",""))
            #else:
            #    dead("Pattern not found, skipping lines!")
        # Merge lists into one dict
        cpu_dict = dict(zip(card_core_name, card_core_load))
        # Convert dict into pandas DF
        dict_df = pd.DataFrame(list(cpu_dict.items()))
        # Drop column by its index (first col)
        dict_df = dict_df.drop(dict_df.columns[0], axis=1)
        # Transpose the dataframe, set the first column as index
        #  so we won't have index column as the uppermost row
        dict_df_t = dict_df.set_index(dict_df.columns[0]).T
        # Append to temp file for further processing
        with open(npu_util_log_temp, 'a') as csv_outfile:
            dict_df_t.to_csv(csv_outfile, sep=',', index=False)
        return card_core_name

def main():
    node_config_file = "/home/backup/scripts/node.json"
    timenow = time.strftime("%d-%m-%Y-%H-%M") + "-00"
    node, ip_addr, user, password = load_node_config(node_config_file)
    daynow = time.strftime("%d-%m-%Y")
    i = len(node)
    get_npu_util = Pool(i).starmap(node_connect_log_queries, zip(node, ip_addr,
                                   user, password, repeat(timenow), repeat(daynow)))

if __name__ == "__main__":
    main()
