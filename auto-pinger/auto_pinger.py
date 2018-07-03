#!/usr/local/bin/python3

import argparse
import json
import pexpect
import ipaddress
import sys
from progress.bar import Bar
import pandas as pd
import subprocess
import time

def load_node_config(node_conf_file, node):
    with open(node_conf_file) as nodefile:
        loaded = json.load(nodefile)
        ip_address = ''
        user = ''
        password = ''
        # populate list
        for item in loaded:
            if item['nodename'] == node:
                ip_address = item['ip_address']
                user = item['user']
                password = item['password']
        return ip_address, user, password

def load_ip_data_to_dict(ip_conf_file):
    dt = pd.Series.from_csv(ip_conf_file, header=0).to_dict()
    return dt
    
def get_ping(node, ip_address, user, password, ctx, src_ip, ip_dict, ip_logfile):
    node_expect = node + "#"
    ssh_command = "ssh " + user + "@" + ip_address
    ssh_newkey = "Are you sure you want to continue connecting"
    bar = Bar('Processing', max=len(ip_dict))
    child = pexpect.spawnu(ssh_command)
    child_resp = child.expect([pexpect.TIMEOUT, ssh_newkey, "password"])
    with open(ip_logfile,'w') as child_log:
        child.logfile = child_log
        if child_resp == 0:
            print("SSH could not login. Here is what SSH said:")
            print(child.before, child.after)
            return None
        elif child_resp == 1:
            child.sendline("yes")
            child_resp_nest = child.expect([pexpect.TIMEOUT, "password:"])
            if child_resp_nest == 0:
                print("SSH could not login. Here is what SSH said:")
                print(child.before, child.after)
            elif child_resp_nest == 1:
                child.sendline(password)
                child.expect(node_expect)
                child.sendline("context " + ctx)
                child.expect(node_expect)
                for k, v in ip_dict.items():
                    if "/" in str(v):
                        n = ipaddress.ip_network(v)
                        v_addr = str(next(n.hosts()))
                        child.sendline("ping " + v_addr + " src " + src_ip + " count 1")
                        child.expect(node_expect)
                    else:
                        child.sendline("ping " + str(v) + " src " + src_ip + " count 1")
                        child.expect(node_expect)
                    bar.next()
                bar.finish()
                try:
                    child.expect(node_expect, timeout=10)
                    child.sendline("exit")
                except pexpect.TIMEOUT:
                    print("Pattern not found! Exiting anyway...")
        else:
            child.sendline(password)
            child.expect(node_expect)
            child.sendline("context " + ctx)
            child.expect(node_expect)
            for k, v in ip_dict.items():
                if "/" in str(v):
                    n = ipaddress.ip_network(v)
                    v_addr = str(next(n.hosts()))
                    child.sendline("ping " + v_addr + " src " + src_ip + " count 1")
                    child.expect(node_expect)
                else:
                    child.sendline("ping " + str(v) + " src " + src_ip + " count 1")
                    child.expect(node_expect)
                bar.next()
            bar.finish()
            try:
                child.expect(node_expect, timeout=10)
                child.sendline("exit")
            except pexpect.TIMEOUT:
                print("Pattern not found! Exiting anyway...")

def parse_ip_ping_log(ip_log_parsed, ip_dict, output_file):
    with open(ip_log_parsed, 'r') as infile:
        lines = infile.readlines()
        num_of_node = len(ip_dict)
        reachable_count = 0
        unreachable_count = 0
        for k, v in ip_dict.items():
            if "/" in v:
                n = ipaddress.ip_network(v)
                search_ip_addr = str(next(n.hosts()))
                search_ip_name = k
                for index, line in enumerate(lines):
                    result = ""
                    if search_ip_addr in line:
                        line_result = lines[index + 1]
                        if "1 received" in line_result:
                            result = "Reachable"
                            reachable_count += 1
                        elif "0 received" in line_result:
                            result = "Unreachable"
                            unreachable_count +=1
                        message = "Ping result toward:{}, with IP:{}, is:{}"
                        print_to_file_and_stdout(message.format(search_ip_name, search_ip_addr, result), output_file)
                        time.sleep(.300)
            else:
                search_ip_addr = str(v)
                search_ip_name = k
                for index, line in enumerate(lines):
                    result = ""
                    if search_ip_addr in line:
                        line_result = lines[index + 1]
                        if "1 received" in line_result:
                           result = "Reachable"
                           reachable_count += 1
                        elif "0 received" in line_result:
                           result = "Unreachable"
                           unreachable_count += 1
                        message = "Ping result toward:{}, with IP:{}, is:{}"
                        print_to_file_and_stdout(message.format(search_ip_name, search_ip_addr, result), output_file)
                        time.sleep(.300)
            ping_success_rate = (reachable_count / (reachable_count + unreachable_count))*100            
        return ping_success_rate, reachable_count, unreachable_count
      
def argument_list():
    parser = argparse.ArgumentParser(description="A StarOS simple automated pinger program")
    parser.add_argument("-n", "--node", help="choose the desired node")
    parser.add_argument("-c", "--ctx", help="choose the desired source context")
    parser.add_argument("-s", "--src", help="choose the desired source ip address")
    parser.add_argument("-i", "--ifile", help="choose the IP address list file")
    args = parser.parse_args()
    # Handle case where there is no argument fed to the script
    if not len(sys.argv) > 1:
        print("")
        print("Script Aborted! See below notes:")
        print("")
        parser.print_help()
    node = str(args.node).upper()
    ctx = str(args.ctx)
    src_ip = str(args.src)
    inputfile = args.ifile
    return node, ctx, src_ip, inputfile

def print_to_file_and_stdout(text, output_file):
    with open(output_file, 'a') as OF:
        print(text)
        OF.write(text + "\n")

def main():
    start_time = time.time()
    node_config_file = "/home/backup/scripts/node.json"
    node, ctx, src_ip, inputfile = argument_list()
    timenow = time.strftime("%d-%m-%Y-%H-%M")
    ip_logfile = "/home/backup/ping_test/" + ctx + "_log.txt"
    ip_log_parsed = "/home/backup/ping_test/" + ctx + "_log_parsed.txt"
    final_log_file = "/home/backup/ping_test/ping-test-" + node + "-" + ctx + "-" + src_ip + "-" + timenow + ".txt"
    ip_address, user, password = load_node_config(node_config_file, node)
    ip_dict = load_ip_data_to_dict(inputfile)
    subprocess.call("clear", shell=True)
    get_ping_name = get_ping.__name__
    print("Executing " + get_ping_name + " in " + node + "....")
    print("Using source IP address " + src_ip + " in context: " + ctx)
    get_ping(node, ip_address, user, password, ctx, src_ip, ip_dict, ip_logfile)
    print("Log found in " + ip_logfile)
    print("Tidying up log...")
    subprocess.call("grep -A1 \"ping statistics\" " + ip_logfile + " > " + ip_log_parsed, shell=True)
    print("Parsing log...")
    print("Saving log file to " + final_log_file)
    print("")
    print_to_file_and_stdout("=======================PING TEST RESULT=======================", final_log_file)
    print_to_file_and_stdout("", final_log_file)
    ping_success_rate, reachable_count, unreachable_count = parse_ip_ping_log(ip_log_parsed, ip_dict, final_log_file)
    print_to_file_and_stdout("==============================================================", final_log_file)
    message = "STATS --> PING_SR:{}%, REACHABLE:{}, UNREACHABLE:{}"
    print_to_file_and_stdout(message.format(ping_success_rate, reachable_count, unreachable_count), final_log_file)
    print("")
    finish_time = time.time()
    duration_script = finish_time - start_time
    print("Program finished execution in " + str(duration_script) + " seconds")

if __name__ == "__main__":
    main()
