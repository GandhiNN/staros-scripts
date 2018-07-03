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

nodeList = ['10.205.62.4', '10.205.67.4', '10.205.57.4']
sshList = [ssh1,ssh2,ssh3]
threads = []
username = 'cisco.candresa'
password = 'password*1'



def login(node, ssh):

        #print 'logged in'
        logfile1 = open('logfile1-' + node + '.txt', 'a+')
        logfile2 = open('logfile2-' + node + '.txt', 'a+')
        count = 0
        starttime = time.time()
        while count < 24:
                ssh.connect(node, username=username, password=password)
                chan = ssh.invoke_shell()
                resp = chan.recv(10000)
                #print resp
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        #print 'R1',resp
                chan.send('cli test-commands password boxer\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        #print 'R2',resp
                chan.send('show messenger usage table all\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        logfile1.write(resp.decode('utf-8'))
                logfile1.write('\n')
                chan.send('show cloud monitor di-network detail\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        logfile2.write(resp.decode('utf-8'))
                logfile2.write('\n')
                chan.send('show cloud performance dinet pps\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        logfile2.write(resp.decode('utf-8'))
                logfile2.write('\n')
                chan.send('show cloud performance port\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        logfile2.write(resp.decode('utf-8'))
                logfile2.write('\n')
                chan.send('show iftask stats summary\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        logfile2.write(resp.decode('utf-8'))
                logfile2.write('\n')
                chan.send('show cpu info verbose\n')
                resp = ''
                while '>' not in str(resp):
                        #chan.send('\n')
                        resp = chan.recv(10000)
                        logfile2.write(resp.decode('utf-8'))
                logfile2.write('\n')
                chan.send('exit\n')
                count = count + 1
                if count == 24 :
                        break
                time.sleep(3600.0 - ((time.time() - starttime) % 3600.0))
        logfile1.close()
        logfile2.close()


for node,ssh_ele in zip(nodeList,sshList):
        t = threading.Thread(target = login, args=(node,ssh_ele,))
        t.start()
        threads.append(t)
for t in threads:
        t.join()
