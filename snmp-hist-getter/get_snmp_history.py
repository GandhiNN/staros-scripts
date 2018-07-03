#!/usr/local/bin/python3

import json
import pexpect
import subprocess
import time
import os
from datetime import datetime, timedelta
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

def node_connect_snmp_query(node, ipaddr, user, password, timenow):
    base_dir = "/home/backup/snmp_trap_history/log/"
    base_log = base_dir + node + "-snmp-trap-history-" + timenow + ".log"
    with open(base_log, 'w') as basefile:
        node_expect = node + "#"
        ssh_command = "ssh " + user + "@" + ipaddr
        snmp_query_cmd = "show snmp trap history verbose" 
        try:
            child = pexpect.spawnu(ssh_command)
            child.logfile = basefile
            child.expect("password:")
            child.sendline(password)
            try:
                child.expect(node_expect)
                child.sendline(snmp_query_cmd)
                snmp_query_resp = child.expect([pexpect.EOF, pexpect.TIMEOUT], timeout=150)
                if snmp_query_resp == "0":
                    child.expect(node_expect)
                    child.sendline("exit")
                else:
                    print("Timeout reached! Exiting...")
            except:
                pass
        except:
            pass

def copy_log(log_in, log_out):
    subprocess.call("cat " + log_in + " >> " + log_out, shell=True)

def remove_dupl_lines(log_in, log_out):
    # Sort alphabetically, based on month and day name
    subprocess.call("cat " + log_in + " | grep -i \"internal trap notification\" | sort -k2,1M -k3,3n | uniq > " + log_out, shell=True)

def beautify_log(node, nodetype, timenow, last_hour, current_day):
    print("Current day is: ", current_day)
    print("Current hour is: ", timenow)
    base_dir = "/home/backup/snmp_trap_history/log/"
    recycle_bin = "/home/backup/snmp_trap_history/recycle_bin/"
    backup_dir = "/home/backup/snmp_trap_history/backup/"
    snmp_log_dir = "/home/backup/snmp_trap_history/snmp_dir/"
    current_log = base_dir + node + "-snmp-trap-history-" + timenow + ".log"
    final_log_raw = base_dir + node + "-snmp-trap-history-" + current_day + "-raw.log"
    final_log = base_dir + node + "-snmp-trap-history-" + current_day + ".log"
    yesterday_raw = datetime.now() - timedelta(days=1)
    yesterday = yesterday_raw.strftime("%d-%m-%Y")
    final_log_raw_yesterday = base_dir + node + "-snmp-trap-history-" + yesterday + "-raw.log"
    final_log_yesterday = base_dir + node + "-snmp-trap-history-" + yesterday + ".log"
    # check how much seconds have passed since midnight
    now = datetime.now()
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, 
                              microsecond=0)).total_seconds()
    if not os.path.isfile(final_log):
        if not os.path.isfile(final_log_raw):
            print("Final raw log not found! Creating...")
            subprocess.call("touch " + final_log_raw, shell=True)
        print("Final log not found! Creating...")
        subprocess.call("touch " + final_log, shell=True)
    else:
        print("File found! Appending to existing file...")
        print("Final Raw log found! Appending to existing file...")
        copy_log(current_log, final_log_raw)
        subprocess.call("mv " + current_log + " " + backup_dir, shell=True)
    if seconds_since_midnight < 3600: # if day has changed, corner cases
        print("Day has changed!")
        remove_dupl_lines(final_log_raw_yesterday, final_log_yesterday)
        subprocess.call("mv " + final_log_raw_yesterday + " " + recycle_bin, shell=True)
        subprocess.call("mv " + final_log_yesterday + " " + snmp_log_dir + "/" + nodetype + "/" \
                         + node + "/", shell=True)
    else:
        print("Still at the same day!")

if __name__ == '__main__':
    node_config_file = "/home/backup/scripts/node.json"
    node, nodetype, ipaddr, user, password = load_node_config(node_config_file)
    timenow = time.strftime("%d-%m-%Y-%H") + "-00-00"
    last_hour_datetime = datetime.now() - timedelta(hours=1)
    time_last_hour = last_hour_datetime.strftime("%d-%m-%Y-%H")
    last_hour = str(time_last_hour) + "-00-00"
    current_day_datetime = datetime.now().strftime("%d-%m-%Y")
    current_day = str(current_day_datetime)
    #print(current_day)
    i = len(node)
    get_snmp = Pool(i).starmap(node_connect_snmp_query, zip(node, ipaddr, user, 
                                                       password, repeat(timenow)))
    write_log = Pool(i).starmap(beautify_log, zip(node, nodetype, repeat(timenow), repeat(last_hour), 
                                           repeat(current_day)))


