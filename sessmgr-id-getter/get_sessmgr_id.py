#!/usr/local/bin/python3

import smtplib
import time
import jinja2
import threading
import os
import csv

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from sshtunnel import SSHTunnelForwarder
from paramiko import client
from operator import itemgetter
from matplotlib import pyplot as plt

class ssh:
    client = None

    def __init__(self, address, username, password):
        print('Connecting to target node...')
        self.client = client.SSHClient()
        self.client.set_missing_host_key_policy(client.AutoAddPolicy())
        self.client.connect(address, username=username, password=password, look_for_keys=False)

    def sendCommand(self, command, logpath):
        if (self.client):
            stdin, stdout, stderr = self.client.exec_command(command)
            log_output = stdout.read()
            stdin.flush()
            log_output_file = open(logpath, 'wb')
            log_output_file.write(log_output)
            log_output_file.close()
        else:
            print('Connection is not opened...')

class sshThread(threading.Thread):

    def __init__(self, threadId, node, ip_addr, user, password):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.node = node
        self.ip_addr = ip_addr
        self.user = user
        self.password = password

    def run(self):
        print("Starting " + self.name)
        # Get lock to synchronize threads
        threadLock.acquire()
        connectNode(self.node, self.ip_addr, self.user, self.password)
        # Free lock to release next thread
        threadLock.release()

def connectNode(node, ip_addr, user, password):
    command_message = 'show task resources'
    output_log = '/home/edruser/scripts/{}-tasklog'.format(node)
    connection_node = ssh(ip_addr, user, password)
    connection_node.sendCommand(command_message, output_log)

def parseOutput(logpath, hournow, node):
    # Dict generator
    items = []
    nodename = node
    cpu = ''
    facility = ''
    task_instance = ''
    cpu_used = ''
    cpu_alloc = ''
    mem_used = ''
    mem_alloc = ''
    files_used = ''
    files_alloc = ''
    status = ''
    hournow = hournow
    
    with open(logpath, 'r') as infile:
        for line in infile:
            if 'sessmgr' and 'I' in line:
                cpu = str(line.strip().split()[0])
                facility = str(line.strip().split()[1])
                task_instance = int(line.strip().split()[2])
                cpu_used = str(line.strip().split()[3])
                cpu_alloc = str(line.strip().split()[4])
                mem_used = str(line.strip().split()[5])
                mem_alloc = str(line.strip().split()[6])
                files_used = str(line.strip().split()[7])
                files_alloc = str(line.strip().split()[8])
                status = str(line.strip().split()[12])

                # Dict = {}
                # We just have to quote the keys as needed
                node_data = dict(nodename=nodename, cpu=cpu, facility=facility, task_instance=task_instance,
                    cpu_used=cpu_used, cpu_alloc=cpu_alloc, mem_used=mem_used,
                    mem_alloc=mem_alloc, files_used=files_used, files_alloc=files_alloc,
                    status=status, hournow=hournow)
                items.append(node_data)
    # Sort list of dict, descending, by task_instance id value
    # and get the top 10
    items_by_task_id = sorted(items, key=itemgetter('task_instance'), reverse=True)
    items_by_task_id_top10 = items_by_task_id[:10]
    return items_by_task_id_top10

def createCsv(fileinput, node_highest_dict):
    with open(fileinput, 'a+') as csv_infile:   # append mode with read mode implied
        # File has been opened at this point
        csv_infile.seek(0)  # Ensure that we are at the start of the file
        first_char = csv_infile.read(1) # Get the first char of the file
        w = csv.DictWriter(csv_infile, node_highest_dict.keys())    # Set the dict writer object
        if not first_char:  # If first char is not found then deem as empty file
            w.writeheader()
            w.writerow(node_highest_dict)
        else:   # non-empty file
            w.writerow(node_highest_dict)

def sendEmail(sender, recipient, subject, html_skel, *args):
    # Create message container - the correct MIME type is
    # multipart/alternative
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    if type(recipient) is list:
        msg['To'] = recipient[0]
        msg['Cc'] = ','.join(recipient[1:])
        msg['Bcc'] = recipient[-1]
    else:
        msg['To'] = recipient
        msg['Cc'] = ''
        msg['Bcc'] = ''

    # Create the body of the message
    text = 'Please find below the top 10 SessMgr Instance ID for All Cisco SGSN/MME Nodes'

    # Record the MIME types of both parts
    # text/plain and text/html
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html_skel, 'html')

    # Attach image to email body
    with open('/home/edruser/scripts/sessmgr_plot/sessmgr_trending.png', 'rb') as f:
        img = f.read()
        msgImg = MIMEImage(img, 'png')
        msgImg.add_header('Content-ID', '<image1>')
        msgImg.add_header('Content-Disposition', 'inline', filename='/home/edruser/scripts/sessmgr_plot/sessmgr_trending.png')

    # Attach parts into message container
    # According to RFC 2046, the last part of a multipart message
    #   in this case the HTML message
    #   is best and preferred
    msg.attach(part1)
    msg.attach(part2)
    msg.attach(msgImg)

    try:
        server = smtplib.SMTP('127.0.0.1', 10022)
        server.ehlo()
        server.sendmail(sender, recipient, msg.as_string())
        server.close()
        print('Successfully sent the mail')
    except:
        print('Failed to send the mail')

# Main
datenow = time.strftime('%s')
datenow_human = time.strftime('%a, %d %b %Y %H:%M:00')
datenow_subj = time.strftime('%Y-%m-%d %H:%M:00')
hournow = time.strftime('%H:00')
datenow_dateonly = time.strftime('%Y%m%d')
datenow_dateonly_dashed = time.strftime('%Y-%m-%d')

# Threading definition
threadLock = threading.Lock()
threads = []

# Create new ssh threads
thread1 = sshThread(1, 'VSGBTR05', '10.205.57.4', 'psi.gandhi', 'password*1')
thread2 = sshThread(2, 'VSGCBT04', '10.205.62.4', 'psi.gandhi', 'password*1')
thread3 = sshThread(3, 'VSGCBT05', '10.205.67.4', 'psi.gandhi', 'password*1')

# Start new ssh threads
thread1.start()
thread2.start()
thread3.start()

# Add threads to thread list
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)

# Wait for all threads to complete
for t in threads:
    t.join()
print('Exiting main thread')

# Parse log and generate list of dict container
# to be used later by Jinja2 template engine
output_log_vsgbtr05 = '/home/edruser/scripts/VSGBTR05-tasklog'
output_log_vsgcbt04 = '/home/edruser/scripts/VSGCBT04-tasklog'
output_log_vsgcbt05 = '/home/edruser/scripts/VSGCBT05-tasklog'
vsgcbt04_dict = parseOutput(output_log_vsgcbt04, hournow, 'VSGCBT04')
vsgbtr05_dict = parseOutput(output_log_vsgbtr05, hournow, 'VSGBTR05')
vsgcbt05_dict = parseOutput(output_log_vsgcbt05, hournow, 'VSGCBT05')

# HTML template engine 
loader = jinja2.FileSystemLoader('/home/edruser/scripts/templateSessmgr.jinja2.html')
env = jinja2.Environment(loader=loader)
template = env.get_template('')

# Render for each node
# manipulate using data contained in list of lists of dict
big_items = []
big_items.append(vsgbtr05_dict)
big_items.append(vsgcbt04_dict)
big_items.append(vsgcbt05_dict)
node_table_html = template.render(items=big_items) 

# Print to csv file
#  mode = append
vsgbtr05_highest_dict = vsgbtr05_dict[0]
vsgcbt04_highest_dict = vsgcbt04_dict[0]
vsgcbt05_highest_dict = vsgcbt05_dict[0]
vsgbtr05_highest_csv = '/home/edruser/scripts/sessmgr_trending_csv/vsgbtr05_highest_sessmgr_{}.csv'.format(datenow_dateonly)
vsgcbt04_highest_csv = '/home/edruser/scripts/sessmgr_trending_csv/vsgcbt04_highest_sessmgr_{}.csv'.format(datenow_dateonly)
vsgcbt05_highest_csv = '/home/edruser/scripts/sessmgr_trending_csv/vsgcbt05_highest_sessmgr_{}.csv'.format(datenow_dateonly)

# Create CSV
createCsv(vsgbtr05_highest_csv, vsgbtr05_highest_dict)
createCsv(vsgcbt04_highest_csv, vsgcbt04_highest_dict)
createCsv(vsgcbt05_highest_csv, vsgcbt05_highest_dict)

# Matplotlib graph generation
with open(vsgbtr05_highest_csv, 'r') as f_vsgbtr05:
    with open(vsgcbt04_highest_csv, 'r') as f_vsgcbt04:
        with open(vsgcbt05_highest_csv, 'r') as f_vsgcbt05:
            data_vsgbtr05 = list(csv.reader(f_vsgbtr05))
            data_vsgcbt04 = list(csv.reader(f_vsgcbt04))
            data_vsgcbt05 = list(csv.reader(f_vsgcbt05))

task_instance_btr05 = [i[3]
                       for i in data_vsgbtr05[1::]] # skip header
time_instance = [i[11]
                       for i in data_vsgbtr05[1::]] # skip header
task_instance_cbt04 = [i[3]
                       for i in data_vsgcbt04[1::]] # skip header
task_instance_cbt05 = [i[3]
                       for i in data_vsgcbt05[1::]] # skip header

# Instantiate plot object
# which consists of 3 subplots
fig = plt.figure()

ax1 = fig.add_subplot(311) # first subplot in three
ax1.plot(time_instance, task_instance_btr05, 'ro') # red dot
# Set title for the whole graph
ax1.set_title('SessMgr Task Instance ID Changes Over Time - {}'.format(datenow_dateonly_dashed))
ax1.legend(['VSGBTR05'], loc='upper left') # subplot legend

ax2 = fig.add_subplot(312, sharex=ax1) # second subplot in three, use ax1's x-ticks
ax2.plot(time_instance, task_instance_cbt04, 'bs') # blue square
ax2.legend(['VSGCBT04'], loc='upper left')
ax2.set_ylabel('SessMgr Task Instance ID') # hacky hack to set a common-Y label across all subplots

ax3 = fig.add_subplot(313, sharex=ax1) # third subplot in three, use ax1's x-ticks
ax3.plot(time_instance, task_instance_cbt05, 'g^') # green triangle
ax3.legend(['VSGCBT05'], loc='upper left')

# Add legends and other graph properties
plt.xticks(rotation=30) # Rotate x-label 30 degrees
plt.setp(ax1.get_xticklabels(), visible=False) # Remove x ticks from ax1 subplot
plt.setp(ax2.get_xticklabels(), visible=False) # Remove x ticks from ax2 subplot
plt.xlabel('Hour')
plt.tight_layout() # Use tight layout in the graph
plt.savefig('/home/edruser/scripts/sessmgr_plot/sessmgr_trending.png')

# Create HTML Skeleton
html_skel = """\
    <html>
        <head>
            <style>
                table, th, td {{
                    border: 1px solid black;
                    border-collapse: collapse;
                    text-align: center;
                    table-layout: auto;
                }}
                .safe-limit {{ color: green; }}
                .warn-limit {{ color: orange; }}
                .over-limit {{ color: red; }}
            </style>
        </head>
        <body>
            <article>
                <header>
                    <p>Please find below the top 10 SessMgr Instance ID for All Cisco SGSN/MME Nodes as per {}</p>
                </header>
            </article>
            {}<br>
            <h3><b>Trending Graph</b></h3>
            <img src="cid:image1">
        </body>
        <footer>
            <p><i>This email is automatically generated. <strong>Please do not reply</strong>.</i></p>
            <p><i>Contact Information: <a
                href="mailto:ngakan.gandhi@packet-systems.com">ngakan.gandhi@packet-systems.com</a>.</i></p>
        </footer>
    </html>
    """.format(datenow_human, node_table_html)

# Write raw HTML file as point of reference
html_file = open('/home/edruser/scripts/sessMgr_table.html', 'w')
html_file.write(html_skel)
html_file.close()

# Forward local port to XL SMTP mail port
with SSHTunnelForwarder(
    ('10.23.33.125', 22),
    ssh_username = 'root',
    ssh_password = 'psi12345',
    remote_bind_address = ('10.17.6.210', 25),
    local_bind_address = ('127.0.0.1', 10022)
) as tunnel:
    recipient = ['ciscopsi-minims-xlvpc@external.cisco.com', 'asaudale@cisco.com', 
         'samrony.fauzi@packet-systems.com', 'teguh.adiputra@packet-systems.com','frando.manurung@packet-systems.com',
         'alfa.manaf@packet-systems.com', 'jordia.ibrahim@packet-systems.com', 'david.hasibuan@packet-systems.com',
         'mohamad.sanusi@packet-systems.com', 'qinthara.tohari@packet-systems.com', 'mokhamad.yanuar@gmail.com',
         'psikokas@gmail.com']
    sendEmail('goss.alertinfo@xl.co.id', recipient,
        '[PSI/Cisco Internal] TOP 10 SessMgr Instance ID Cisco SGSN/MME {}'.format(datenow_subj), html_skel)

# Remove source logs
os.remove(output_log_vsgcbt04)
os.remove(output_log_vsgbtr05)
os.remove(output_log_vsgcbt05)
print('FINISH EXECUTING SCRIPT')
