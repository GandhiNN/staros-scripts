#!/usr/local/bin/python3

from multiprocessing.dummy import Pool
from datetime import date, timedelta
from itertools import repeat
import subprocess
import os
import json
import logging
import pexpect
import time
import argparse

def node_connect_save_ssd(node, ip_address, user, password, timenow, timeyesterday):
    save_log = "/home/backup/scripts/logs/nodeconnectsavessd-" + node + "-" + timenow + ".log"
    save_log_yesterday = "/home/backup/scripts/logs/nodeconnectsavessd-" + node + "-" + timeyesterday + ".log" 
    with open(save_log, 'w') as expect_log:
        node_expect = node + "#"
        ssh_command = "ssh " + user + "@" + ip_address
        ssd_dir = "/flash/sftp/ssd/"
        save_ssd = "show support details to file " + ssd_dir + \
                    node + "-" + timenow + ".ssd" + " compress -noconfirm"
        ssh_newkey = "Are you sure you want to continue connecting"
        logging.debug("Starting SSH Command toward %s" % node + "...")
        child = pexpect.spawnu(ssh_command)
        child.logfile = expect_log
        #Set SSH conditional
        resp = child.expect([pexpect.TIMEOUT, ssh_newkey, "password:"])
        if resp == 0: # Timeout
            print("SSH failed! Here is what SSH said:")
            print(child.before, child.after)
            return None
        elif resp == 1: # SSH missing public key
            child.sendline("yes")
            respnest = child.expect([pexpect.TIMEOUT, "password:"])
            if respnest == 0: # Timeout
                print("SSH failed! Here is what SSH said:")
                print(child.before, child.after)
            elif respnest == 1: # Continue
                child.sendline(password)
                child.expect(node_expect)
                logging.debug("Saving support file in %s" % node + "...")
                child.sendline(save_ssd)
                child.expect(node_expect, timeout=None)
                child.sendline("exit")
                logging.debug("Saving SSD in node: " + node + " successful! Exiting...")
        elif resp == 2: # SSH has the public key 
            child.sendline(password)
            child.expect(node_expect)
            logging.debug("Saving support file in %s" % node + "...")
            child.sendline(save_ssd)
            child.expect(node_expect, timeout=None)
            child.sendline("exit")
            logging.debug("Save support file in node: " + node + " successful! Exiting...")

def node_pull_ssd(node, ip_address, user, password, timenow, timeyesterday):
    pull_log = "/home/backup/scripts/logs/nodepullssd-" + node + "-" + timenow + ".log"
    pull_log_yesterday = "/home/backup/scripts/logs/nodepullssd-"  + node + "-" + timeyesterday + ".log"
    with open(pull_log, 'w') as expect_log:
        node_expect = node + "#"
        sftp_dir = "/sftp/ssd/" 
        sftp_local_dir = "/home/backup/ssd/temp/"
        file_pattern = node + "-" + timenow + ".ssd.tar.gz"
        file_pattern_yesterday = node + "-" + timeyesterday + ".ssd.tar.gz"
        sftp_command = "sftp " + user + "@" + ip_address + ":" + sftp_dir
        sftp_pull_command = "get " + file_pattern
        sftp_delete_command = "rm -f " + file_pattern_yesterday
        ssh_newkey = "Are you sure you want to continue connecting"
        os.chdir(sftp_local_dir)
        logging.debug("Changing local dir to %s" % sftp_local_dir)
        child = pexpect.spawnu(sftp_command)
        child.logfile = expect_log
        logging.debug("Pulling generated support files from %s" % node + "...")
        # Set conditional based on matched patterns returned by "i" pexpect instance
        i = child.expect([pexpect.TIMEOUT, ssh_newkey, "password:"])
        if i == 0: # Timeout
            print("SFTP could not login. Here is what SFTP said:")
            print(child.before, child.after)
            return None
        if i == 1: # SSH does not have the public key
            child.sendline("yes")
            b = child.expect([pexpect.TIMEOUT, "password:"])
            if b == 0: # Timeout
                print("ERROR!")
                print("SFTP Could not login. Here is what SFTP said:")
                print(child.before, child.after)
            if b == 1: # Continue
                child.sendline(password)
                child.expect("sftp>")
                child.sendline(sftp_pull_command)
                c = child.expect([pexpect.TIMEOUT, "No such file or directory", "sftp>"], timeout=300)
                if c == 0: # Timeout
                    logging.debug("SFTP Timeout!")
                    print(child.before, child.after)
                elif c == 1: # File not found, still try to delete yesterday's file
                    logging.debug("File not found! Exiting...")
                    child.sendline(sftp_delete_command)
                    child.expect("sftp>")
                    child.sendline("bye")
                    logging.debug("Exiting...")
                elif c ==2: # delete and bye
                    child.sendline(sftp_delete_command)
                    child.expect("sftp>")
                    child.sendline("bye")
                    logging.debug("Exiting...")            
        if i == 2: # SSH already has the public key
            child.sendline(password)
            child.expect("sftp>")
            child.sendline(sftp_pull_command)
            c = child.expect([pexpect.TIMEOUT, "No such file or directory", "sftp>"], timeout=300)
            if c == 0: # Timeout
                logging.debug("SFTP Timeout!")
                print(child.before, child.after)
            elif c == 1: # File not found
                logging.debug("File not found! Exiting...")
                child.sendline(sftp_delete_command)
                child.expect("sftp>")
                child.sendline("bye")
                logging.debug("Exiting...")
            elif c == 2: # bye
                child.sendline(sftp_delete_command)
                child.expect("sftp>")
                child.sendline("bye")     
                logging.debug("Exiting...")

# Load node user and password configuration file
def load_node_config(node_config_file):
    with open(node_config_file) as node_confile:
        return json.load(node_confile)

def get_node_user_pass(node_config, node_type):
    node = []
    ip_address = []
    user = []
    password = []
    # populate lists
    for item in node_config:
        if item["nodetype"] == node_type:
            node.append(item['nodename'])
            ip_address.append(item['ip_address'])
            user.append(item['user'])
            password.append(item['password'])
    return node, ip_address, user, password 

def move_file(node, node_type, timenow):
    basedir = "/home/backup/ssd/" + node_type + "/"
    tempdir = "/home/backup/ssd/temp/"
    ssd_file = node + "-" + timenow + ".ssd.tar.gz"
    source = tempdir + ssd_file 
    target = basedir + node + "/" + ssd_file
    logging.debug("Moving support file from %s to %s" % (source, target))
    subprocess.call(["mv", source, target])
    subprocess.call(["rm", "-rf", source])

def main():
    NODE_MAP = {'SGSNMME': "Cisco STAROS-Based SGSN/MME",
                'GGSNSPGW': "Cisco STAROS-Based GGSN/SPGW"
               }
    parser = argparse.ArgumentParser(description="Select node to be queried")
    parser.add_argument("-n", "--node", choices=NODE_MAP.keys(),
                        help="choose the desired node")
    args = parser.parse_args()
    node_type = str(args.node)
    last_day = date.today() - timedelta(1)
    timenow = time.strftime("%d-%m-%Y-%H") + "-00-00"
    timeyesterday = last_day.strftime("%d-%m-%Y-%H") + "-00-00" 
    node_config_file = "/home/backup/scripts/node.json"
    node_config = load_node_config(node_config_file)
    node, ip_address, user, password = get_node_user_pass(node_config, node_type)
    i = len(node)
    logging.basicConfig(level=logging.DEBUG, format='(Thread %(funcName)s %(message)s')
    print("### LOGGING STARTS ###")
    print("Script execution start time : ", time.ctime())
    savessd = Pool(i).starmap(node_connect_save_ssd, zip(node, ip_address, user, password, repeat(timenow), repeat(timeyesterday))) # avoid using timenow as zip iterator
    pullssd = Pool(i).starmap(node_pull_ssd, zip(node, ip_address, user, password, repeat(timenow), repeat(timeyesterday))) # avoid using timenow as zip iterator
    movessd = Pool(i).starmap(move_file, zip(node, repeat(node_type), repeat(timenow))) # avoid using nodeType as zip iterator
    print("Script execution finish time : ", time.ctime())
    print("### LOGGING FINISHED ###")

if __name__ == "__main__":
    main()

