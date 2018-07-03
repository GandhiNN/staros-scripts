import paramiko,time,datetime, re, os
import threading
from threading import Thread, Lock
_db_lock = Lock()
import sys
import time

ssh1 = paramiko.SSHClient()
ssh1.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh2 = paramiko.SSHClient()
ssh2.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh3 = paramiko.SSHClient()
ssh3.set_missing_host_key_policy(paramiko.AutoAddPolicy())

nodeList = ['10.205.62.4', '10.205.67.4','10.205.57.4']
sshList = [ssh1,ssh2,ssh3]
threads = []
username = 'cisco.candresa'
password = 'password*1'

def login(node, ssh):
        
        #print 'logged in'
        count = 0
        starttime = time.time()
        while count != 192:
                ssh.connect(node, username=username, password=password)
                chan = ssh.invoke_shell()
                resp = chan.recv(10000)
                #print resp
                while '>' not in str(resp):
                        chan.send('\n')
                        resp = chan.recv(10000)
                        #print resp
                chan.send('cli test-commands password boxer\n')
                resp = ''
                resp = chan.recv(10000)
                #print resp
                while '>' not in str(resp):
                        chan.send('\n')
                        resp = chan.recv(10000)
                        #print resp
                chan.send('task snap facility bulkstat all\n')
                count = count + 1
                resp = ''
                resp = chan.recv(10000)
                while '>' not in str(resp):
                        chan.send('\n')
                        resp = chan.recv(10000)
                chan.send('exit\n')
                time.sleep(900.0 - ((time.time() - starttime) % 900.0))



for node,ssh_ele in zip(nodeList,sshList):
        t = threading.Thread(target = login, args=(node,ssh_ele,))
        t.start()
        threads.append(t)
for t in threads:
        t.join()
