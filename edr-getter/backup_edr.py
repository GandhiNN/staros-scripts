#!/usr/local/bin/python3

import time
import threading
import os
import paramiko

class sftpThread(threading.Thread):

    def __init__(self, threadId, host, port, username, password, node, datenow):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.node = node
        self.datenow = datenow

    def run(self):
        print("Starting " + self.name)
        # Get lock to synchronize threads
        threadLock.acquire()
        getSftp(self.host, self.port, self.username, self.password, self.node, self.datenow)
        # Free lock to release next thread
        threadLock.release()

def getSftp(host, port, username, password, node, datenow):
    sftp = None
    transport = None
    local_dir = '/data/edrbackup/SGSNMME/' + node + '/'
    os.chdir(local_dir)

    try:
        transport = paramiko.Transport((host, port))
        transport.connect(None, username, password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.chdir('/hd-raid/records/event')
        remote_dir_list = sftp.listdir('.')
        #edr_local_path = '/home/edruser/edrbackup/SGSNMME/' + node + '/'
        edr_local_path = '/data/edrbackup/SGSNMME/' + node + '/'
        edr_local_files = [f for f in os.listdir(edr_local_path)]
        diff_files = [f for f in remote_dir_list
                      if f not in edr_local_files]
        edr_local_files_len = len(edr_local_files)
        remote_file_len = len(diff_files)
        print('There are {} new EDR files in {}! Executing Pull Sequence...'.format(remote_file_len, datenow))
        
        for item in diff_files:
            print('Pulling {} from {}...'.format(item, node))
            sftp.get(item, item)

        remove_empty_files(local_dir)
        sftp.close()

    except Exception as e:
        print('An error occured creating SFTP client : %s: %s' %
            (e.__class__, e))
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()
        pass

def remove_empty_files(local_dir):
    filelist = [f for f in os.listdir(local_dir) if os.stat(f).st_size == 0]
    for f in filelist:
        print('Removing {} from {}...'.format(f, local_dir))
        os.remove(f)

# MAIN
datenow = time.strftime('%Y-%m-%d %H:%M:%S')
port = 22

# Threading definition
threadLock = threading.Lock()
threads = []

# Create SFTP threads
thread1 = sftpThread(1, '10.205.57.4', port, 'psiuser', 'Psi12345!', 'VSGBTR05', datenow)
thread2 = sftpThread(2, '10.205.62.4', port, 'psiuser', 'Psi12345!', 'VSGCBT04', datenow)
thread3 = sftpThread(3, '10.205.67.4', port, 'psiuser', 'Psi12345!', 'VSGCBT05', datenow)
thread4 = sftpThread(4, '10.205.87.4', port, 'psiuser', 'Psi12345!', 'VSGBTR06', datenow)

# Start new sftp threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()

# Add threads to thread list
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)

# Wait for all threads to complete
for t in threads:
    t.join()

print('Exiting main thread')
