#!/usr/local/bin/python3

import csv
import glob
import json
import pexpect
import subprocess
import time
from multiprocessing.dummy import Pool
from itertools import repeat

def load_node_config(node_conf_file):
    with open(node_conf_file) as nodefile:
        loaded = json.load(nodefile)
        node = []
        nodetype = []
        ip_address = []
        user = []
        password = []
        # populate list
        for item in loaded:
            node.append(item['nodename'])
            nodetype.append(item['nodetype'])
            ip_address.append(item['ip_address'])
            user.append(item['user'])
            password.append(item['password'])
        return node, nodetype, ip_address, user, password

def load_apn_config(apn_file):
    with open(apn_file) as apnfile:
        loaded = json.load(apnfile)
        apn = []
        plmn = []
        country = []
        # populate list
        for item in loaded:
            apn.append(item['apn'])
            mcc = item['mcc']
            mnc = item['mnc']
            plmn.append(mcc + mnc)
            country.append(item['country'])
        return apn, plmn, country

def node_connect_imsi_check(node, nodetype, ipaddr, user, password, mcc_mnc, timenow):
    base_dir = "/home/backup/inroamer_qci_enhanced/"
    for mccmnc in mcc_mnc:
        base_log = base_dir + "raw/" + node + "-" + mccmnc + "-" + timenow + ".log"
        with open(base_log, 'w') as basefile:
            node_expect = node + "#"
            ssh_command = "ssh " + user + "@" + ipaddr
            if nodetype == "SGSNMME":
                check_imsi_cmd = "show mme-service session full imsi " + mccmnc + "* | grep QCI" 
            elif nodetype == "GGSNSPGW":
                check_imsi_cmd = "show subscribers saegw-only full imsi " + mccmnc + "* | grep QCI"
            try:
                child = pexpect.spawnu(ssh_command)
                child.logfile = basefile
                child.expect("password:")
                child.sendline(password)
                try:
                    child.expect(node_expect)
                    child.sendline(check_imsi_cmd)
                    check_imsi_resp = child.expect([pexpect.EOF, pexpect.TIMEOUT], timeout=300)
                    if check_imsi_resp == "0":
                        child.expect(node_expect)
                        child.sendline("exit")
                    else:
                        print("Timeout reached! Exiting...")
                except:
                    pass
            except:
                pass

def get_qci_mme(log):
    with open(log, 'r') as parse_log:
        qci_8_count_mme = 0
        qci_9_count_mme = 0
        for line in parse_log:
            if "QCI: 8" in line:
                qci_8_count_mme += 1
            elif "QCI: 9" in line:
                qci_9_count_mme += 1
    return qci_8_count_mme, qci_9_count_mme

def get_qci_sgw(log):
    with open(log, 'r') as parse_log:
        qci_8_count_sgw = 0
        qci_9_count_sgw = 0
        for line in parse_log:
            if "QCI              : 8" in line:
                qci_8_count_sgw += 1
            elif "QCI              : 9" in line:
                qci_9_count_sgw += 1
    return qci_8_count_sgw, qci_9_count_sgw

def csv_write(node, node_type, apn_config_file, timenow):
    base_dir = "/home/backup/inroamer_qci_enhanced/"
    csv_log = base_dir + "csv/" + node + "-qcibreakdown-" + timenow + ".csv"
    with open(csv_log, 'w') as fin_csv:
        writer = csv.writer(fin_csv)
        writer.writerow (['apn_name', 'plmn', 'country', 'qci8_count', 'qci9_count'])
        with open(apn_config_file) as apnfile:
            loaded = json.load(apnfile)
            for item in loaded:
                apn = item['apn']
                mcc = item['mcc']
                mnc = item['mnc']
                plmn = mcc + mnc
                country = item['country']
                try:
                    raw_log = base_dir + "raw/" + node + "-" + plmn + "-" + timenow + ".log"
                    if node_type == "SGSNMME":
                        qci_8_count_mme, qci_9_count_mme = get_qci_mme(raw_log)
                        writer.writerow([apn, plmn, country, qci_8_count_mme, qci_9_count_mme])
                    elif node_type == "GGSNSPGW":
                        qci_8_count_sgw, qci_9_count_sgw = get_qci_sgw(raw_log)
                        writer.writerow([apn, plmn, country, qci_8_count_sgw, qci_9_count_sgw])
                except FileNotFoundError:
                    writer.writerow(['null','null','null','null','null'])

if __name__ == '__main__':
    apn_config_file = "/home/backup/scripts/apn_roaming.json"
    node_config_file = "/home/backup/scripts/node.json"
    apn_name, mcc_mnc, country_list = load_apn_config(apn_config_file)
    node, nodetype, ipaddr, user, password = load_node_config(node_config_file)
    timenow = time.strftime("%d-%m-%Y-%H") + "-00-00"
    i = len(node)
    get_imsi = Pool(i).starmap(node_connect_imsi_check, zip(node, nodetype, ipaddr, user, 
                                                       password, repeat(mcc_mnc), repeat(timenow)))
    write_csv = Pool(i).starmap(csv_write, zip(node, nodetype, repeat(apn_config_file), repeat(timenow)))
    # CSV Housekeeping
    recycle_bin_dir = "/home/backup/inroamer_qci_enhanced/recycle_bin/"
    archive_dir = "/home/backup/inroamer_qci_enhanced/archive/"
    tar_file = "/home/backup/inroamer_qci_enhanced/csv/qci-csv-allnode-" + timenow + ".tar.gz"
    csv_path = "/home/backup/inroamer_qci_enhanced/csv/"
    subprocess.call("tar czf " + tar_file + " " + "-C" + csv_path + " .", shell=True)
    subprocess.call("mv " + csv_path + "*.csv " + recycle_bin_dir, shell=True)
    subprocess.call("mv " + tar_file + " " + archive_dir, shell=True)

