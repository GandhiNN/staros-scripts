#!/usr/local/bin/python3

from multiprocessing.dummy import Pool
from datetime import date, timedelta
import subprocess
import os
import json
import logging
import pexpect
import time

def node_connect_save_config(node, ip_address, user, password, nodetype):
    with open("/home/backup/scripts/logs/nodeconnectsaveconfig.log", 'w') as expect_log:
        node_expect = node + "#"
        timenow = time.strftime("%d-%m-%Y-%H") + "-00-00"
        ssh_command = "ssh " + user + "@" + ip_address
        save_config = "save configuration /flash/sftp/cfg/" + node + \
                     "-" + timenow + ".cfg" + " -noconfirm"
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
        if resp == 1: # SSH missing public key
            child.sendline("yes")
            respnest = child.expect([pexpect.TIMEOUT, "password:"])
            if respnest == 0: # Timeout
                print("SSH failed! Here is what SSH said:")
                print(child.before, child.after)
            if respnest == 1: # Continue
                child.sendline(password)
                child.expect(node_expect)
                logging.debug("Saving current configuration in %s" % node + "...")
                child.sendline(save_config)
                save_config_resp = child.expect([pexpect.EOF, pexpect.TIMEOUT])
                if save_config_resp == 0:
                    if nodetype == "SGSNMME":
                        child.expect(node_expect, timeout=30)
                    elif nodetype == "GGSNSPGW":
                        child.expect(node_expect, timeout=60)
                    child.sendline("exit")
                    logging.debug("Save Config in node: " + node + " successful! Exiting...")
                elif save_config_resp == 1:
                    logging.debug("Timeout reached! Exiting...")
        if resp == 2: # SSH has the public key 
            child.sendline(password)
            child.expect(node_expect)
            child.sendline(save_config)
            save_config_resp = child.expect([pexpect.EOF, pexpect.TIMEOUT])
            if save_config_resp == 0:
                if nodetype == "SGSNMME":
                    child.expect(node_expect, timeout=30)
                elif nodetype == "GGSNSPGW":
                    child.expect(node_expect, timeout=60)
                child.sendline("exit")
                logging.debug("Save Config in node: " + node + " successful! Exiting...")
            elif save_config_resp == 1:
               logging.debug("Timeout reached! Exiting...")

def node_pull_config(node, ip_address, user, password, nodetype):
    with open("/home/backup/scripts/logs/nodepullconfig.log", 'w') as expect_log:
        node_expect = node + "#"
        last_day = date.today() - timedelta(1)
        timenow = time.strftime("%d-%m-%Y-%H") + "-00-00"
        time_yesterday = last_day.strftime("%d-%m-%Y-%H") + "-00-00" 
        sftp_dir = "/sftp/cfg/" 
        sftp_local_dir = "/home/backup/config/temp/"
        file_pattern = node + "-" + timenow + ".cfg"
        file_pattern_yesterday = node + "-" + time_yesterday + ".cfg"
        sftp_command = "sftp " + user + "@" + ip_address + ":" + sftp_dir
        sftp_pull_command = "get " + file_pattern
        sftp_del_command = "rm " + file_pattern_yesterday
        ssh_newkey = "Are you sure you want to continue connecting"
        os.chdir(sftp_local_dir)
        logging.debug("Changing local dir to %s" % sftp_local_dir)
        child = pexpect.spawnu(sftp_command)
        child.logfile = expect_log
        logging.debug("Pulling generated configuration from %s" % node + "...")
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
                print("FTP Could not login. Here is what SFTP said:")
                print(child.before, child.after)
            if b == 1: # Continue
                child.sendline(password)
                child.expect("sftp>")
                child.sendline(sftp_pull_command)
                if nodetype == "SGSNMME":
                    child.expect("sftp>", timeout=30)
                elif nodetype == "GGSNSPGW":
                    child.expect("sftp>", timeout=60)
                child.sendline(sftp_del_command)
                c = child.expect([pexpect.TIMEOUT, "No such file or directory"])
                if c == 0: # Timeout
                    logging.debug("SFTP Timeout!")
                    print(child.before, child.after)
                if c == 1: # File not found
                    logging.debug("File not found! Exiting...")
                    child.sendline("bye")
                # Else file found and available for deletion
                child.expect("sftp>")
                child.sendline("bye")
                logging.debug("Exiting...")            
        if i == 2: # SSH already has the public key
            child.sendline(password)
            child.expect("sftp>")
            child.sendline(sftp_pull_command)
            if nodetype == "SGSNMME":
                child.expect("sftp>", timeout=30)
            elif nodetype == "GGSNSPGW":
                child.expect("sftp>", timeout=60)
            child.sendline(sftp_del_command)
            logging.debug("Deleting previous day configuration from %s" % node)
            c = child.expect([pexpect.TIMEOUT, "No such file or directory"])
            if c == 0: # Timeout
                logging.debug("SFTP Timeout!")
                print(child.before, child.after)
            if c == 1: # File not found
                logging.debug("File not found! Exiting...")
                child.sendline("bye")
                logging.debug("Exiting...")
            # Else file found and available for deletion
            child.expect("sftp>")
            child.sendline("bye")     
            logging.debug("Exiting...") 

# Load node user and password configuration file
def load_node_config(node_config_file):
    with open(node_config_file) as node_con_file:
        return json.load(node_con_file)

def get_node_user_pass(node_config):
    node = []
    ip_address = []
    user = []
    password = []
    nodetype = []
    # populate lists
    for item in node_config:
        node.append(item['nodename'])
        ip_address.append(item['ip_address'])
        user.append(item['user'])
        password.append(item['password'])
        nodetype.append(item['nodetype'])
    return node, ip_address, user, password, nodetype 

def move_file(node, nodetype):
    timenow = time.strftime("%d-%m-%Y-%H") + "-00-00"
    basedir = "/home/backup/config/" + nodetype + "/"
    tempdir = "/home/backup/config/temp/"
    conf_file = node + "-" + timenow + ".cfg"
    source = tempdir + conf_file 
    target = basedir + node + "/" + conf_file
    logging.debug("Moving configuration file from %s to %s" % (source, target))
    subprocess.call(["mv", source, target])
    subprocess.call(["rm", "-rf", source])

def main():
    node_config_file = "/home/backup/scripts/node.json"
    node_config = load_node_config(node_config_file)
    node, ip_address, user, password, nodetype = get_node_user_pass(node_config)
    i = len(node)
    logging.basicConfig(level=logging.DEBUG, format='(Thread %(funcName)s %(message)s')
    saveconfig = Pool(i).starmap(node_connect_save_config, zip(node, ip_address, user, password, nodetype))
    pullconfig = Pool(i).starmap(node_pull_config, zip(node, ip_address, user, password, nodetype))
    moveconfig = Pool(i).starmap(move_file, zip(node, nodetype))

if __name__ == "__main__":
    main()

