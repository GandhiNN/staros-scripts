#!/usr/local/bin/python3
#
# get_sdr_v18.py
# author : ngakan.gandhi@packet-systems.com
# date : 07 Jan 2018

# Import modules
import time
import paramiko
import json
import os
import gzip
import re
import sqlite3
import csv
import pandas as pd
import tarfile
from itertools import repeat
from multiprocessing import Pool
from sqlalchemy import create_engine

# Load the node login credentials
def getNodeLogin(node_config_file):
    with open(node_config_file) as node_confile:
        node_config = json.load(node_confile)
        node = []
        ip_addr = []
        user = []
        password = []
        # Populate lists
        for item in node_config:
            if 'SGSNMME' in item['nodetype']:
                node.append(item['nodename'])
                ip_addr.append(item['ip_address'])
                user.append(item['user'])
                password.append(item['password'])
        return node, ip_addr, user, password

def getSdr(node, host, username, password):
    sftp = None
    transport = None
    port = 22
    local_dir = '/home/backup/sdr/SGSNMME/' + node + '/'
    os.chdir(local_dir)

    try:
        # Create transport object, doesn't start the channel yet
        transport = paramiko.Transport((host, port)) # sock-like tuple
        transport.connect(hostkey=None, username=username,
             password=password)
        # Create SFTP client channel from an open Transport object
        sftp = paramiko.SFTPClient.from_transport(transport)
        # Change dir to SDR dir on remote machine
        sftp.chdir('/hd-raid/support/records/')
        item_remote = 'sdr.0.gz'
        item_local = 'sdr.0.gz'
        print('Pulling {} as {}...'.format(item_remote, item_local))
        sftp.get(item_remote, item_local)
        sftp.close()
        return item_local

    except Exception as e:
        print('An error occured creating SFTP client : {}: {}'.format(e.__class__, e))
        # If SFTP channel has been created, force close
        if sftp is not None:
            sftp.close()
        # If Transport object has been created, force delete
        if transport is not None:
            transport.close()
        pass

def check_f_tstamp(node, gz_ifile):
    # Get the filemembers of the gunzipped SDR files
    t = tarfile.open(gz_ifile, 'r')
    file_list = t.getnames()
    file_flag = file_list[0]
    # Get the file creation timestamp (all numerical digits)
    file_tstamp_list = re.findall(r'\d+', file_flag)
    file_tstamp_elem = file_tstamp_list[0]
    tstamp_pattern = '%Y%m%d%H%M'
    file_tstamp = int(time.mktime(time.strptime(file_tstamp_elem, tstamp_pattern)) + (7 * 3600))
    file_time_flag = '/home/backup/sdr/SGSNMME/' + node + '/flag/file.flag'
    try:
        fout = open(file_time_flag, 'r')
        fout_time = [x for x in fout.read().split()]
        print('Last {0} SDR filename fetched is {1}'.format(node, fout_time[0]))
        print('Current File: {0}'.format(file_flag))
        fout.close()
        if str(file_flag) == str(fout_time[0]):
            print("New {0} SDR file still not generated! Skipping file collection...".format(node))
            local_dir = '/home/backup/sdr/SGSNMME/' + node + '/'
            os.chdir(local_dir)
            try:
                print("Deleting garbage...")
                os.remove('sdr.0.gz')
            except:
                pass
            return 0
        else:
            fout = open(file_time_flag, 'w')
            fout.write(file_flag)
            fout.close()
            print("Updating {0} Filename Flag Reference to {1}".format(node, file_flag))
    except FileNotFoundError:
        fout = open(file_time_flag, 'w')
        fout.write(file_flag)
        fout.close
        print("Creating {0} Filename Flag Reference...".format(node))
        return 0
    return file_tstamp

def unarchiveGunzip(gz_file, nodename, file_time):
    raw_dir = '/home/backup/sdr/SGSNMME/' + nodename + '/raw/'
    sdr_unarchived = gz_file.replace('.gz', '') + '-' + str(file_time) + '.txt'
    sdr_unarchived_name = raw_dir + sdr_unarchived
    f = gzip.open(gz_file, 'rb')
    g = open(sdr_unarchived_name, 'wb')
    print('Unarchiving {} into {}...'.format(gz_file, sdr_unarchived_name))
    g.write(f.read())
    f.close()
    g.close()
    os.remove(gz_file)
    return sdr_unarchived_name

def create_headers_conf(headers_csv, json_output):
    csv_rows = []
    with open(headers_csv) as infile:
        reader = csv.DictReader(infile)
        title = reader.fieldnames
        for row in reader:
            csv_rows.extend([{title[i]:row[title[i]]
                             for i in range(len(title))}])
        write_json(csv_rows, json_output)

def write_json(data, output_file):
    with open(output_file, 'w') as f:
        f.write(json.dumps(data, sort_keys=False,
                          indent=4, separators=(',',': '),
                          ensure_ascii=False))

def unpack_json(json_file):
    with open(json_file) as f:
        config = json.load(f)
        flag_name = []
        flag_members = []
        prefix = []
        for item in config:
            flag_name.append(item['flag_name'])
            flag_members.append(item['flag_members'])
            prefix.append(item['prefix'])
        return flag_name, flag_members, prefix

def get_emm_ctl_msg(sdr_txt): # sdr_txt = text file ; file_time = epoch
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        emm_ctl_data = re.search('^Total EMM Control Messages:.*LPP Payload Type.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0)
        # Replace/Remove Unneeded Char Sequences
        emm_ctl_data = re.sub(r'(.) ', r'\1', emm_ctl_data) # Remove single whitespace
        emm_ctl_data = re.sub(r'-', r'_', emm_ctl_data) # Replace hyphen with underscores
        emm_ctl_data = re.sub(r'/', r'_', emm_ctl_data) # Replace / with underscores
        emm_ctl_data = re.sub(r':', r' ', emm_ctl_data) # Replace colon with a single whitespace - handle tight spacing between
                                                        # char and longint value
        # Get all numeric data value
        value = [int(s) for s in emm_ctl_data.split() 
                 if s.isdigit()]
        header = [str(s) for s in emm_ctl_data.split()
                  if not s.isdigit()]
        return header, value

def get_esm_ctl_msg(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        esm_ctl_data = re.search('^Total ESM Control Messages:.*ESM Data Transport.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0)
        # Replace/Remove Unneeded Char Sequences
        esm_ctl_data = re.sub(r'(.) ', r'\1', esm_ctl_data) # Remove single whitespace
        esm_ctl_data = re.sub(r'-', r'_', esm_ctl_data) # Replace hyphen with underscores
        esm_ctl_data = re.sub(r'/', r'_', esm_ctl_data) # Replace / with underscores
        esm_ctl_data = re.sub(r':', r' ', esm_ctl_data) # Replace colon with a single whitespace
        # Get all headers
        header = [str(s) for s in esm_ctl_data.split()
                  if not s.isdigit()]
        # Get all numeric values
        value = [int(s) for s in esm_ctl_data.split() 
                 if s.isdigit()]
        return header, value

def get_disc_reason(sdr_txt, file_time):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # Get all disconnect reasons
        # Note -> .*?(?:#|$) matches a string until end of line
        disc_reason_data = re.search('^Unknown\(0\).*sx-invalid-response\(650\).*?(?:#|$)', all_data, 
            re.DOTALL|re.MULTILINE).group(0)
        disc_reason_data = re.sub(r'-', r'_', disc_reason_data) # Replace hyphen with underscores
        disc_reason_data = re.sub(r'/', r'_', disc_reason_data) # Replace forward slash with underscores
        disc_reason_data = re.sub("\(.*\)", "", disc_reason_data) # Remove enum code of disc_reason
        lines = disc_reason_data.splitlines()
        fin_list = []
        for item in lines:
            item = item.split()
            fin_list.append(item)
        header_disc_reason = ['Timestamp']
        value_disc_reason = [file_time]
        for item in fin_list:
            header_disc_reason.append(item[0])
            value_disc_reason.append(item[1])
    return header_disc_reason, value_disc_reason

def get_gmm_sm_attached_subs(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        gmm_sm_attached_data = re.search('^Attached Subscribers:.*3G-ISR-Activated.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0)
        # Replace/Remove Unneeded Char Sequences
        gmm_sm_attached_data = re.sub(r'(.) ', r'\1', gmm_sm_attached_data) # Remove single whitespace
        gmm_sm_attached_data = re.sub(r'-', r'_', gmm_sm_attached_data) # Replace hyphen with underscores
        gmm_sm_attached_data = re.sub(r'/', r'_', gmm_sm_attached_data) # Replace / with underscores
        gmm_sm_attached_data = re.sub(r':', r' ', gmm_sm_attached_data) # Replace colon with a single whitespace
        #print(gmm_sm_attached_data)
        # Get all headers
        header = [str(s) for s in gmm_sm_attached_data.split()
                  if not s.isdigit()]
        # Get all numeric values
        value = [int(s) for s in gmm_sm_attached_data.split() 
                 if s.isdigit()]
        return header, value

def get_gmm_sm_active_subs(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # note : re.search will find until the last line matching the specified patterns
        # not the first line encountered
        gmm_sm_actv_data = re.search('^Activated Subscribers:.*Activated HSPA PDP Contexts.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0)
        # Replace/Remove Unneeded Char Sequences
        gmm_sm_actv_data = re.sub(r'(.) ', r'\1', gmm_sm_actv_data) # Remove single whitespace
        gmm_sm_actv_data = re.sub(r'-', r'_', gmm_sm_actv_data) # Replace hyphen with underscores
        gmm_sm_actv_data = re.sub(r'/', r'_', gmm_sm_actv_data) # Replace / with underscores
        gmm_sm_actv_data = re.sub(r':', r' ', gmm_sm_actv_data) # Replace colon with a single whitespace
        gmm_sm_actv_data = re.sub(r'\(', r'', gmm_sm_actv_data) # Remove opening parenthese
        gmm_sm_actv_data = re.sub(r'\)', r'', gmm_sm_actv_data) # Remove closing parenthese
        # Get all headers
        header = [str(s) for s in gmm_sm_actv_data.split()
                  if not s.isdigit()]
        # Get all numeric values
        value = [int(s) for s in gmm_sm_actv_data.split() 
                 if s.isdigit()]
        return header, value

def get_gmm_sm_attach_req(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # note : re.search will find until the last line matching the specified patterns
        # not the first line encountered
        gmm_sm_attach_data_init = re.search('^Attach Request:.*Total-Attach-Accept.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0)      
        gmm_sm_attach_data_fin = re.search('^Attach Request:.*Ret-3G-Req-Without-LAPI.*?(?:#|$)',
                                    gmm_sm_attach_data_init, re.DOTALL|re.MULTILINE).group(0)   
        # Replace/Remove Unneeded Char Sequences
        gmm_sm_attach_data_fin = re.sub(r'(.) ', r'\1', gmm_sm_attach_data_fin) # Remove single whitespace
        gmm_sm_attach_data_fin = re.sub(r'-', r'_', gmm_sm_attach_data_fin) # Replace hyphen with underscores
        gmm_sm_attach_data_fin = re.sub(r'/', r'_', gmm_sm_attach_data_fin) # Replace / with underscores
        gmm_sm_attach_data_fin = re.sub(r':', r' ', gmm_sm_attach_data_fin) # Replace colon with a single whitespace
        # Get all headers
        header = [str(s) for s in gmm_sm_attach_data_fin.split()
                  if not s.isdigit()]
        # Handling special case for 3G_Att_Req_Without_LAPI334929252Request
        # Which somehow joined with its value
        # Maybe related with the tight spacing between char and numeric
        # May have to do it somewhere else accordingly
        for idx, item in enumerate(header):
            if "3G_Att_Req_Without_LAPI" in item:
                num_val = re.sub("[^0-9]", "", item)
                num_val = num_val[1:] # Get rid of the 3 from "3G"
                item = re.sub("\d+", "", item)
                header[idx] = item
        # Get all numeric values
        value = [int(s) for s in gmm_sm_attach_data_fin.split() 
                 if s.isdigit()]
        #return header, value, num_val
        return header, value

def get_gmm_sm_attach_accept(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # note : re.search will find until the last line matching the specified patterns
        # not the first line encountered
        gmm_sm_attach_accept = re.search('^Attach Accept:.*3G-Attach-Complete.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0)        
        # Replace/Remove Unneeded Char Sequences
        gmm_sm_attach_accept = re.sub(r'(.) ', r'\1', gmm_sm_attach_accept) # Remove single whitespace
        gmm_sm_attach_accept = re.sub(r'-', r'_', gmm_sm_attach_accept) # Replace hyphen with underscores
        gmm_sm_attach_accept = re.sub(r'/', r'_', gmm_sm_attach_accept) # Replace / with underscores
        gmm_sm_attach_accept = re.sub(r':', r' ', gmm_sm_attach_accept) # Replace colon with a single whitespace
        # Get all headers
        header = [str(s) for s in gmm_sm_attach_accept.split()
                  if not s.isdigit()]
        # Get all numeric values
        value = [int(s) for s in gmm_sm_attach_accept.split() 
                 if s.isdigit()]
        return header, value

def get_gmm_sm_attach_rej_fail(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # note : re.search will find until the last line matching the specified patterns
        # not the first line encountered
        gmm_sm_attach_rej_fail = re.search('^Attach Reject:.*Internal Triggers:.*?(?:#|$)',
                                    all_data, re.DOTALL|re.MULTILINE).group(0) 
        # Replace/Remove Unneeded Char Sequences
        gmm_sm_attach_rej_fail = re.sub(r'(.) ', r'\1', gmm_sm_attach_rej_fail) # Remove single whitespace
        gmm_sm_attach_rej_fail = re.sub(r'-', r'_', gmm_sm_attach_rej_fail) # Replace hyphen with underscores
        gmm_sm_attach_rej_fail = re.sub(r'/', r'_', gmm_sm_attach_rej_fail) # Replace / with underscores
        # Get all headers
        header = [str(s) for s in gmm_sm_attach_rej_fail.split()
                  if not s.isdigit()]
        # Get all numeric values
        value = [int(s) for s in gmm_sm_attach_rej_fail.split() 
                 if s.isdigit()]
        return header, value                                            

def get_emm_statistics(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # note : re.search will find until the last line matching the specified patterns
        # not the first line encountered
        emm_stats = re.search('^EMM Statistics:.*ECM Statistics:.*?(?:#|$)',
                              all_data, re.DOTALL|re.MULTILINE).group(0)         
        # Replace/Remove Unneeded Char Sequences
        emm_stats = re.sub(r'(.) ', r'\1', emm_stats) # Remove single whitespace
        emm_stats = re.sub(r'-', r'_', emm_stats) # Replace hyphen with underscores
        emm_stats = re.sub(r'/', r'_', emm_stats) # Replace / with underscores
        emm_stats = re.sub(r':', r' ', emm_stats) # Replace colon with a single whitespace
        # Get all headers
        header = [str(s) for s in emm_stats.split()
                  if not s.isdigit()]
        # Get all numeric values
        value = [int(s) for s in emm_stats.split() 
                 if s.isdigit()]
        return header, value

def get_ecm_statistics(sdr_txt):
    with open(sdr_txt, 'rt') as infile:
        all_data = infile.read()
        # note : re.search will find until the last line matching the specified patterns
        # not the first line encountered
        ecm_stats = re.search('^ECM Statistics:.*Total EMM Control Messages:.*?(?:#|$)',
                              all_data, re.DOTALL|re.MULTILINE).group(0)         
        # Replace/Remove Unneeded Char Sequences
        ecm_stats = re.sub(r'(.) ', r'\1', ecm_stats) # Remove single whitespace
        ecm_stats = re.sub(r'-', r'_', ecm_stats) # Replace hyphen with underscores
        ecm_stats = re.sub(r'/', r'_', ecm_stats) # Replace / with underscores
        ecm_stats = re.sub(r':', r' ', ecm_stats) # Replace colon with a single whitespace
        # Get all headers
        header = [str(s) for s in ecm_stats.split()
                  if not s.isdigit()]
        # Handling special case for S1releaseforloadrebalancing147983
        #for idx, item in enumerate(header):
        #    if "S1releaseforloadrebalancing" in item:
        #        val_modded = re.sub("[^0-9]", "", item)
        #        val_modded = val_modded[1:] # Get rid of '1' extracted from S1
                                            # If value is 0, then it will be wiped (returned value = '')
                                            #  because the value is already captured somewhere else
        #        header[idx] = 'S1releaseforloadrebalancing'
        # Get all numeric values
        value = [int(s) for s in ecm_stats.split() 
                 if s.isdigit()]
        # return num_val as val_modded to be inserted in cleanups function later
        #return header, value, val_modded
        return header, value

def header_prefixing(header, value, file_time, flag_name, flag_members, prefix):
    n = len(flag_name)
    i = 0
    while(i < n):
        name = flag_name[i]
        members = int(flag_members[i])
        members = members + 1
        pref = prefix[i]
        index = header.index(name)
        for x in range(index+1, index+members):
            header[x] = pref + header[x]
        i += 1 
    header.insert(0, 'Timestamp')
    value.insert(0, file_time) 

def header_suffixing(header, value, file_time, flag_name, flag_members, prefix):
    n = len(flag_name)
    i = 0
    while(i < n):
        name = flag_name[i]
        members = int(flag_members[i])
        members = members + 1
        suff = prefix[i]
        index = header.index(name)
        for x in range(index+1, index+members):
            header[x] = header[x] + suff
        i += 1 
    header.insert(0, 'Timestamp')
    value.insert(0, file_time)

def remove_item_from_list(the_list, item):
    while item in the_list:
        the_list.remove(item)

def header_gmm_sm_attached_cleanups(header, value, flag_name, 
                                    flag_members, prefix, file_time):
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['AttachedSubscribers', 'HomeSubscribers',
                          'VisitingNationalSubscribers', 'VisitingForeignSubscribers',
                          'NetworkSharingSubscribers', 'SubscribersinPMM_REGISTEREDstate',
                          'SubscribersinGPRS_CONNECTEDstate', 'ISRActivatedSubscribers']
    for item in header_remove_list:
        remove_item_from_list(header, item)
    return header, value

def header_gmm_sm_actv_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Modify for 'Subscribers' and 'PDP Contexts' for the naming baseline
    subscribers_flag = 'ActivatedSubscribers'
    pdpctx_flag = 'ActivatePDPContexts'
    hspa_flag = 'ActivatedHSPASubscribers'
    subscribers_idx = header.index(subscribers_flag)
    pdpctx_idx = header.index(pdpctx_flag)
    hspa_idx = header.index(hspa_flag)
    # Suffixing its child counters accordingly
    # For 'Subscribers'
    for i in range(subscribers_idx+1, pdpctx_idx):
        header[i] += 'Subscribers'
    # For 'PDP Contexts'
    for i in range(pdpctx_idx+1, hspa_idx):
        header[i] += 'PDPContexts'
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['ActivatedSubscribers', 'ActivatePDPContexts',
                          'TotalActvPdpCtxPDPContexts', 'TotalActvPdpCtxwithdualaddressPDPContexts',
                          'ActivatedHSPASubscribers', 'ActivatedHSPAPDPContexts']
    for item in header_remove_list:
        remove_item_from_list(header, item)    
    return header, value

def header_value_emm_ctl_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Remove the first line
    # Use the results as reference for subsequent process
    header = header[1:]
    # Modify for SENT & RECEIVED for baseline
    sent_flag = 'Sent'
    received_flag = 'Received'
    sent_index = header.index(sent_flag)
    received_index = header.index(received_flag)
    for i in range(sent_index+1, received_index):
        header[i] = header[i] + 'Sent'
    for i in range(received_index+1, len(header)):
        header[i] = header[i] + 'Received'
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['Sent', 'GenericDownlinkNasTransportSent', 
                          'GUTIReallocationSent', 'Received',
                          'GenericUplinkNasTransportReceived']
    for item in header_remove_list:
        remove_item_from_list(header, item)     
    # Remove 'ServiceRejectCongestionSent' --> there are two entries, index(89) and index(96)
    # We'll remove index(89) (got 0 value) on both header, and value lists
    del header[89]
    del value[89]
    return header, value

def header_value_esm_ctl_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Remove the first element
    # Use the result as reference for subsequent process
    header = header[1:]
    # Modify for SENT and RECEIVED for the naming baseline
    sent_flag = 'Sent'
    rcvd_flag = 'Received'
    sent_idx = header.index(sent_flag)
    rcvd_idx = header.index(rcvd_flag)
    # Suffixing its child counters accordingly
    # For 'Sent'
    for i in range(sent_idx+1, rcvd_idx):
        header[i] += 'Sent'
    # For 'Rcvd'
    for i in range(rcvd_idx+1, len(header)):
        header[i] += 'Received'
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['Sent', 'Received']
    for item in header_remove_list:
        remove_item_from_list(header, item) 
    return header, value

#def header_gmm_sm_attachreq_cleanups(header, value, num_val, flag_name, flag_members, prefix, file_time):
def header_gmm_sm_attachreq_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Modify for 'Subscribers' and 'PDP Contexts' for the naming baseline
    attach_flag = 'AttachRequest'
    attach_idx = header.index(attach_flag)
    # Suffixing its child counters accordingly
    # For 'Subscribers'
    for i in range(attach_idx+1, len(header)):
        header[i] += 'Request'
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['AttachRequest', 'IMSIRequest', 'PTMSIRequest',
                          'Local_PTMSIRequest', 'Remote_PTMSIRequest',
                          'EPCCapabilitySetRequest', 'LowPriorityAccessIndicatorRequest',
                          'RetransmissionRequest', 'IMSIRequest', 'PTMSIRequest',
                          'Local_PTMSIRequest', 'Remote_PTMSIRequest', 'LowPriorityAccessIndicatorRequest']
    for item in header_remove_list:
        remove_item_from_list(header, item)
    # Insert val_modded (value of 'S1releaseforloadrebalancing' into its matching val index)
    # index retrieved dynamically using header keywords
    #idx_modded = header.index('3G_Att_Req_Without_LAPIRequest')
    #if num_val == '':
    #    return header, value
    #else:
    #    value.insert(idx_modded, num_val)
    #    return header, value
    return header, value

def header_gmm_sm_attachaccept_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Modify for 'Subscribers' and 'PDP Contexts' for the naming baseline
    attach_accept_flag = 'AttachAccept'
    attach_accept_idx = header.index(attach_accept_flag)
    # Suffixing its child counters accordingly
    # For 'Subscribers'
    for i in range(attach_accept_idx+1, len(header)):
        header[i] = 'GmmSm' + header[i]
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['AttachAccept', 'GmmSmRetransmission', 'GmmSmAttachComplete']
    for item in header_remove_list:
        remove_item_from_list(header, item)
    return header, value

def header_gmm_sm_attach_rej_fail_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Header cleanups and modification starts here
    # First phase of header cleanups
    # May have to moved to a dedicated header cleanups function
    for item in header:
        if "GPRSandNon_GPRSservice" in item:
            header[header.index(item)] += 'notallowed' 
        elif "Roamingnotallowedin" in item:
            header[header.index(item)] += 'thisLocationArea'
        elif "3G_GPRSservicenotallowed" == item:
            header[header.index(item)] += 'inthisPLMN'
        elif "2G_GPRSservicenotallowed" == item:
            header[header.index(item)] += 'inthisPLMN'    
        elif "Nosuitablecellsin" in item:
            header[header.index(item)] += 'thisLocationArea'
        elif "MSGtypenotcompatible" in item:
            header[header.index(item)] += 'withprotocolstate'
        elif "Messagenotcompatible" in item:
            header[header.index(item)] += 'withprotocolstate'
        elif "3G_IuReleasebefore" in item:
            header[header.index(item)] += 'Attachover'
        # Put the more explicit pattern matching
        # before the less explicit ones
        # To avoid reading mishaps
        # e.g. (FailureDueto is a subset of FailureDuetoOther)
        elif "3G_FailureDuetoOther" in item:
            header[header.index(item)] += 'OngoingProcedure'
        elif "2G_FailureDuetoOther" in item:
            header[header.index(item)] += 'OngoingProcedure'           
        elif "2G_FailureDueto" in item:
            header[header.index(item)] += 'InternalError'   
    # Workaround to avoid duplicates for "ServiceNotAllowed'
    header = [s.strip(':') for s in header] # Remove colon
    # Remove headers which have no value
    header_remove_list_init = ['notallowed', 'thisLocationArea', 
                                'inthisPLMN','withprotocolstate',
                                'OngoingProcedure','Attachover',
                                'InternalError']
    for item in header_remove_list_init:
        remove_item_from_list(header, item)
    # Modify for 'Gprs-Attach-Reject'
    gprs_attach_rej_flag = 'AttachReject'
    gprs_attach_rej_cause_flag = 'Gprs_AttachRejectCauses'
    gprs_attach_net_fail = 'GPRS_AttachNetworkFailureCause'
    gprs_comb_attach_rej_flag = 'Comb_AttachRejectCauses'
    gprs_comb_attach_net_fail = 'Comb_AttachNetworkFailureCause'
    gprs_attach_rej_idx = header.index(gprs_attach_rej_flag)
    gprs_attach_rej_cause_idx = header.index(gprs_attach_rej_cause_flag)
    gprs_attach_net_fail_idx = header.index(gprs_attach_net_fail)
    gprs_comb_attach_rej_idx = header.index(gprs_comb_attach_rej_flag)
    gprs_comb_attach_net_fail_idx = header.index(gprs_comb_attach_net_fail)
    gprs_attach_failure = 'AttachFailure'
    gprs_attach_failure_idx = header.index(gprs_attach_failure)
    # Suffixing its child counters accordingly
    # For 'GPRS Attach Reject'
    for i in range(gprs_attach_rej_idx+1, gprs_attach_rej_cause_idx):
        header[i] = 'Gprs_' + header[i]
    for i in range(gprs_attach_rej_cause_idx+1, gprs_attach_net_fail_idx):
        header[i] = 'Gprs_AttachReject' + header[i]
    for i in range(gprs_attach_net_fail_idx+1, gprs_comb_attach_rej_idx):
        header[i] = 'Gprs_AttachNetworkFail' + header[i]
    for i in range(gprs_comb_attach_rej_idx+1, gprs_comb_attach_net_fail_idx):
        header[i] = 'Gprs_CombAttachReject' + header[i]
    for i in range(gprs_comb_attach_net_fail_idx+1, gprs_attach_failure_idx):
        header[i] = 'Gprs_CombAttachNetworkFail' + header[i]
    for i in range(gprs_attach_failure_idx+1, len(header)):
        header[i] = 'Gprs_AttachFailure' + header[i]
    header_suffixing(header, value, file_time, flag_name, flag_members, prefix)
    header_remove_list_second = ['AttachReject','Gprs_CongestionReject',
                                 'Gprs_AttachRejectCauses','GPRS_AttachNetworkFailureCause',
                                 'Comb_AttachRejectCauses','Comb_AttachNetworkFailureCause',
                                 'AttachFailure','Gprs_AttachFailureGprs_AttachFailureCauses',
                                 'Gprs_AttachFailure3G_IuReleaseBeforeAttachSegregation','Gprs_AttachFailureGPRS_Attach',
                                 'Gprs_AttachFailureComb_AttachFailureCauses','Gprs_AttachFailureComb_Attach'
                                ]
    for item in header_remove_list_second:
        remove_item_from_list(header, item)
    return header, value

def hdr_val_emm_stats_cleanups(header, value, flag_name, flag_members, prefix, file_time):
    # Remove Last Element of the Header list
    header = header[:-1]
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    # Remove headers which have no value
    header_remove_list = ['EMMStatistics', 'EPSAssociationsbyAttachusingIMSI', 'EPSAssociationsforEmergencyBearerServices',
                         'EPSAssociationsbyAttachusingIMEI', 'EPSAssociationsbyAttachusingLocalGUTI',
                         'EPSAssociationsbyAttachusingForeignGUTI', 'EPSAssociationsbyAttachusingP_TMSI',
                         'EPSAssociationsbyTAUusingForeignGUTI', 'EPSAssociationsbyTAUusingP_TMSI',
                         'AssociationsbyCombinedAttachusingIMSI', 'AssociationsbyCombinedAttachusingLocalGUTI',
                         'AssociationsbyCombinedAttachusingForeignGUTI', 'AssociationsbyCombinedAttachusingP_TMSI',
                         'AssociationsbyCombinedTAUusingForeignGUTI', 'AssociationsbyCombinedTAUusingP_TMSI',
                         'Authentications', 'Identity', 'Security', 'GUTIRelocation', 'PeriodicTAU',
                         'NormalTAUwithoutSGWRelocation', 'TAUwithBearerActivation', 'TAUwithSGWRelocation',
                         'CombinedTA_LAUpdatingwithoutSGWRelocation', 'CombinedTA_LAUpdatingwithSGWRelocation',
                         'TAUwithIMSIattachwithoutSGWRelocation', 'TAUwithIMSIattachandSGWRelocation',
                         'DetachesUEInitiated', 'DetachesNWInitiated', 'DetachesHSSInitiated', 'MobileTerminatedLocationService',
                         'NetworkInitiatedLocationService', 'GUTIReallocation']
    for item in header_remove_list:
        remove_item_from_list(header, item)
    return header, value

def hdr_val_ecm_stats_cleanups_first(header, value, flag_name, flag_members, prefix, file_time):
    # Remove Last Element of the Header list
    header = header[:-1]
    # Add 'CSFBStats' flag for naming baseline
    emm_stats_csfb_flag = 'CSFBStatistics'
    emm_stats_csfb_idx = header.index(emm_stats_csfb_flag)
    # Prefixing its child counters accordingly
    for i in range(emm_stats_csfb_idx+1, len(header)):
        header[i] = 'CSFBStats' + header[i]
    # Header cleanups and modification starts here
    header_prefixing(header, value, file_time, flag_name, flag_members, prefix)
    return header, value

#def hdr_val_ecm_stats_cleanups_second(header, value, val_modded, flag_name, flag_members, midfix, file_time):
def hdr_val_ecm_stats_cleanups_second(header, value, flag_name, flag_members, midfix, file_time):
    # Header cleanups and modification starts here
    # Handle special case for CSFBStats...
    def header_ecm_csfb_midfixing(header, value, file_time, flag_name, flag_members, midfix):
        n = len(flag_name)
        i = 0
        while(i < n):
            name = flag_name[i]
            members = int(flag_members[i])
            members = members + 1
            middle = midfix[i]
            index = header.index(name)
            for x in range(index+1, index+members):
                header[x] = header[x][:9] + middle + header[x][9:]
            i += 1 
    header_ecm_csfb_midfixing(header, value, file_time, flag_name, flag_members, midfix)
    # Remove headers which have no value
    header_remove_list = ['ECMStatistics', 'IdleModeEntryEvents', 
                          'UEInitiatedServiceRequestEvents', 'NWInitiatedServiceRequestEvents', 
                          'UEInitiatedCPServiceRequestEvents', 'NWInitiatedCPServiceRequestEvents', 
                          'PagingInitiationforPSQCI_1Events', 'PagingInitiationforPSQCI_2Events', 
                          'PagingInitiationforPSQCI_3Events', 'PagingInitiationforPSQCI_4Events', 
                          'PagingInitiationforPSQCI_5Events', 'PagingInitiationforPSQCI_6Events', 
                          'PagingInitiationforPSQCI_7Events', 'PagingInitiationforPSQCI_8Events', 
                          'PagingInitiationforPSQCI_9Events', 'PagingInitiationforPSARP_1Events', 
                          'PagingInitiationforPSARP_2Events', 'PagingInitiationforPSARP_3Events', 
                          'PagingInitiationforPSARP_4Events', 'PagingInitiationforPSARP_5Events', 
                          'PagingInitiationforPSARP_6Events', 'PagingInitiationforPSARP_7Events', 
                          'PagingInitiationforPSARP_8Events', 'PagingInitiationforPSARP_9Events', 
                          'PagingInitiationforPSARP_10Events', 'PagingInitiationforPSARP_11Events', 
                          'PagingInitiationforPSARP_12Events', 'PagingInitiationforPSARP_13Events', 
                          'PagingInitiationforPSARP_14Events', 'PagingInitiationforPSARP_15Events', 
                          'PagingInitiationforPSAPN_Profilebasedselection', 'PagingInitiationforCSVoiceEvents', 
                          'PagingInitiationforCSSMSEvents', 'PagingInitiationforCSOtherEvents', 
                          'PagingInitiationforSIGNALINGDETACHEvents', 'PagingInitiationforSIGNALINGLCSEvents', 
                          'PagingInitiationforSIGNALINGIPNEEvents', 'PagingInitiationforSIGNALINGNodeRestorationEvents', 
                          'PagingInitiationforSIGNALINGIdrEvents', 'CSFBStatistics', 
                          'CSFBStatsUEInitiatedVoiceProcedures', 'CSFBStatsUEInitiatedPriorityVoiceProcedures', 
                          'CSFBStatsNWInitiatedVoiceProcedures', 'CSFBStatsNWInitiatedPriorityVoiceProcedures', 
                          'CSFBStatsUEInitiatedSMSProcedures', 'CSFBStatsNWInitiatedSMSProcedures', 
                          'CSFBStatsUEInitiatedIMSIDetaches', 'CSFBStatsNWInitiatedIMSIDetaches']
    for item in header_remove_list:
        remove_item_from_list(header, item)
    # Insert val_modded (value of 'S1releaseforloadrebalancing' into its matching val index)
    # index retrieved dynamically using header keywords
    #idx_modded = header.index('S1releaseforloadrebalancing')
    #if val_modded = '':
    #    return header, value
    #else:
    #    value.insert(idx_modded, val_modded)
    #    return header, value
    return header, value

def write_csv_load_to_db(nodename, header, value, db_table_name, file_time):
    temp_csv = '/home/backup/sdr/SGSNMME/' + nodename + '/{}-temp-{}.csv'.format(db_table_name, file_time)
    db_filename = '/home/backup/sdr/web/' + '{}-sdr.db'.format(nodename)
    with open(temp_csv, 'a+') as f:
        f.seek(0)
        first_char = f.read(1)
        writer = csv.writer(f)
        if not first_char:
            writer.writerow(header)
            writer.writerow(value)
        else:
            writer.writerow(value)
    print("CSV File is: {}".format(temp_csv))
    df = pd.read_csv(temp_csv)
    db = create_engine('sqlite:///{}'.format(db_filename))
    df.to_sql(db_table_name, db, if_exists='append', index=False)
    os.remove(temp_csv)
    local_dir = '/home/backup/sdr/SGSNMME/' + nodename + '/'

def db_main_task(nodename, ip_addr, user, password):
    cur_time = int(time.time())
    print("###" * 10)
    print("current epoch time : {0}".format(cur_time))
    print("###" * 10)
    emm_ctl_msg_headers_def = "/home/backup/sdr/config/headers_emm_ctl_msg.csv"
    emm_ctl_msg_headers_conf = "/home/backup/sdr/config/headers_emm_ctl_msg.json"
    esm_ctl_msg_headers_def = "/home/backup/sdr/config/headers_esm_ctl_msg.csv"
    esm_ctl_msg_headers_conf = "/home/backup/sdr/config/headers_esm_ctl_msg.json"
    gmm_sm_attached_subs_def = "/home/backup/sdr/config/headers_gmm_sm_attached_subs.csv"
    gmm_sm_attached_subs_conf = "/home/backup/sdr/config/headers_gmm_sm_attached_subs.json"
    gmm_sm_actv_subs_def = "/home/backup/sdr/config/headers_gmm_sm_actv_subs.csv"
    gmm_sm_actv_subs_conf = "/home/backup/sdr/config/headers_gmm_sm_actv_subs.json"
    gmm_sm_attach_req_def = "/home/backup/sdr/config/headers_gmm_sm_attachreq.csv"
    gmm_sm_attach_req_conf = "/home/backup/sdr/config/headers_gmm_sm_attachreq.json"   
    gmm_sm_attach_accept_def = "/home/backup/sdr/config/headers_gmm_sm_attachaccept.csv"
    gmm_sm_attach_accept_conf = "/home/backup/sdr/config/headers_gmm_sm_attachaccept.json"
    gmm_sm_attach_rej_fail_def = "/home/backup/sdr/config/headers_gmm_sm_attach_rej_fail.csv"
    gmm_sm_attach_rej_fail_conf = "/home/backup/sdr/config/headers_gmm_sm_attach_rej_fail.json" 
    emm_stats_def = "/home/backup/sdr/config/headers_emm_stats.csv"
    emm_stats_conf = "/home/backup/sdr/config/headers_emm_stats.json"
    ecm_stats_def = "/home/backup/sdr/config/headers_ecm_stats.csv"
    ecm_stats_conf = "/home/backup/sdr/config/headers_ecm_stats.json"
    ecm_csfb_stats_def = "/home/backup/sdr/config/headers_ecm_csfb_stats.csv"
    ecm_csfb_stats_conf = "/home/backup/sdr/config/headers_ecm_csfb_stats.json" 
    sdr_raw = getSdr(nodename, ip_addr, user, password)
    f_tstamp = check_f_tstamp(nodename, sdr_raw)
    time.sleep(5)
    if f_tstamp == 0:
        print("Skipping script execution toward to next cron schedule...")
    else:
        sdr_file = unarchiveGunzip(sdr_raw, nodename, f_tstamp)
        create_headers_conf(emm_ctl_msg_headers_def, emm_ctl_msg_headers_conf)
        flag_name_emm_ctl, flag_members_emm_ctl, prefix_emm_ctl = unpack_json(emm_ctl_msg_headers_conf)
        create_headers_conf(esm_ctl_msg_headers_def, esm_ctl_msg_headers_conf)
        flag_name_esm_ctl, flag_members_esm_ctl, prefix_esm_ctl = unpack_json(esm_ctl_msg_headers_conf)
        create_headers_conf(gmm_sm_attached_subs_def, gmm_sm_attached_subs_conf)
        flag_name_gmm_sm_attached_subs, flag_members_gmm_sm_attached_subs, prefix_gmm_sm_attached_subs = unpack_json(gmm_sm_attached_subs_conf)
        create_headers_conf(gmm_sm_actv_subs_def, gmm_sm_actv_subs_conf)
        flag_name_gmmsm_actv_subs, flag_members_gmmsm_actv_subs, prefix_gmmsm_actv_subs = unpack_json(gmm_sm_actv_subs_conf)
        create_headers_conf(gmm_sm_attach_req_def, gmm_sm_attach_req_conf)
        flag_name_gmmsm_attachreq, flag_members_gmmsm_attachreq, prefix_gmmsm_attachreq = unpack_json(gmm_sm_attach_req_conf)
        create_headers_conf(gmm_sm_attach_accept_def, gmm_sm_attach_accept_conf)
        flag_name_gmmsm_attachaccept, flag_members_gmmsm_attachaccept, prefix_gmmsm_attachaccept = unpack_json(gmm_sm_attach_accept_conf)
        create_headers_conf(gmm_sm_attach_rej_fail_def, gmm_sm_attach_rej_fail_conf)
        flag_name_gmmsm_attachrejfail, flag_members_gmmsm_attachrejfail, prefix_gmmsm_attachrejfail = unpack_json(gmm_sm_attach_rej_fail_conf)
        create_headers_conf(emm_stats_def, emm_stats_conf)
        flag_name_emm_stats, flag_members_emm_stats, suffix_emm_stats = unpack_json(emm_stats_conf)
        create_headers_conf(ecm_stats_def, ecm_stats_conf)
        flag_name_ecm_stats, flag_members_ecm_stats, prefix_ecm_stats = unpack_json(ecm_stats_conf)
        create_headers_conf(ecm_csfb_stats_def, ecm_csfb_stats_conf)
        flag_name_ecm_csfb_stats, flag_members_ecm_csfb_stats, prefix_ecm_csfb_stats = unpack_json(ecm_csfb_stats_conf)
        header_emm_ctl, value_emm_ctl = get_emm_ctl_msg(sdr_file)
        header_esm_ctl, value_esm_ctl = get_esm_ctl_msg(sdr_file)
        header_disc_reason, value_disc_reason = get_disc_reason(sdr_file, f_tstamp)
        header_gmm_sm_attached_subs_init, val_gmm_sm_attached_subs_init = get_gmm_sm_attached_subs(sdr_file)
        hdr_gmmsm_actv_init, val_gmmsm_actv_init = get_gmm_sm_active_subs(sdr_file)
        #hdr_gmmsm_attachreq_init, val_gmmsm_attachreq_init, num_val = get_gmm_sm_attach_req(sdr_file) 
        hdr_gmmsm_attachreq_init, val_gmmsm_attachreq_init = get_gmm_sm_attach_req(sdr_file)       
        hdr_gmmsm_attachaccept_init, val_gmmsm_attachaccept_init = get_gmm_sm_attach_accept(sdr_file)
        hdr_gmmsm_attach_rej_fail_init, val_gmmsm_attach_rej_fail_init = get_gmm_sm_attach_rej_fail(sdr_file)
        hdr_emm_stats_init, val_emm_stats_init = get_emm_statistics(sdr_file)
        #hdr_ecm_stats_init, val_ecm_stats_init, val_modded = get_ecm_statistics(sdr_file)
        hdr_ecm_stats_init, val_ecm_stats_init = get_ecm_statistics(sdr_file)
        header_emm_ctl_final, value_emm_ctl_final = header_value_emm_ctl_cleanups(header_emm_ctl, value_emm_ctl, 
                                                        flag_name_emm_ctl, flag_members_emm_ctl, prefix_emm_ctl, f_tstamp)
        header_esm_ctl_final, value_esm_ctl_final = header_value_esm_ctl_cleanups(header_esm_ctl, value_esm_ctl, 
                                                        flag_name_esm_ctl, flag_members_esm_ctl, prefix_esm_ctl, f_tstamp)
        header_gmm_sm_attached_subs_cleaned, val_gmm_sm_attached_subs_cleaned = header_gmm_sm_attached_cleanups(header_gmm_sm_attached_subs_init, 
                                                                                                                val_gmm_sm_attached_subs_init, 
                                                                                                                flag_name_gmm_sm_attached_subs, 
                                                                                                                flag_members_gmm_sm_attached_subs,
                                                                                                                prefix_gmm_sm_attached_subs,
                                                                                                                f_tstamp)
        hdr_gmmsm_actv_fin, val_gmmsm_actv_fin = header_gmm_sm_actv_cleanups(hdr_gmmsm_actv_init, 
                                                                            val_gmmsm_actv_init,
                                                                            flag_name_gmmsm_actv_subs,
                                                                            flag_members_gmmsm_actv_subs,
                                                                            prefix_gmmsm_actv_subs,
                                                                            f_tstamp)
        #hdr_gmmsm_attachreq_fin, val_gmmsm_attachreq_fin = header_gmm_sm_attachreq_cleanups(hdr_gmmsm_attachreq_init, 
        #                                                                 val_gmmsm_attachreq_init,
        #                                                                 num_val,
        #                                                                 flag_name_gmmsm_attachreq,
        #                                                                 flag_members_gmmsm_attachreq,
        #                                                                 prefix_gmmsm_attachreq,
        #                                                                 f_tstamp)
        hdr_gmmsm_attachreq_fin, val_gmmsm_attachreq_fin = header_gmm_sm_attachreq_cleanups(hdr_gmmsm_attachreq_init, 
                                                                         val_gmmsm_attachreq_init,
                                                                         flag_name_gmmsm_attachreq,
                                                                         flag_members_gmmsm_attachreq,
                                                                         prefix_gmmsm_attachreq,
                                                                         f_tstamp)      
        hdr_gmmsm_attachaccept_fin, val_gmmsm_attachaccept_fin = header_gmm_sm_attachaccept_cleanups(hdr_gmmsm_attachaccept_init,
                                                                                                 val_gmmsm_attachaccept_init,
                                                                                                 flag_name_gmmsm_attachaccept,
                                                                                                 flag_members_gmmsm_attachaccept,
                                                                                                 prefix_gmmsm_attachaccept,
                                                                                                 f_tstamp) 
        hdr_gmmsm_attach_rej_fail_fin, val_gmmsm_attach_rej_fail_fin = header_gmm_sm_attach_rej_fail_cleanups(hdr_gmmsm_attach_rej_fail_init,
                                                                                                          val_gmmsm_attach_rej_fail_init,
                                                                                                          flag_name_gmmsm_attachrejfail,
                                                                                                          flag_members_gmmsm_attachrejfail,
                                                                                                          prefix_gmmsm_attachrejfail,
                                                                                                          f_tstamp)    
        hdr_emm_stats_fin, val_emm_stats_fin = hdr_val_emm_stats_cleanups(hdr_emm_stats_init, 
                                                                          val_emm_stats_init, 
                                                                          flag_name_emm_stats, 
                                                                          flag_members_emm_stats, 
                                                                          suffix_emm_stats, 
                                                                          f_tstamp)
        hdr_ecm_stats_fin_init, val_ecm_stats_fin_init = hdr_val_ecm_stats_cleanups_first(hdr_ecm_stats_init, 
                                                                                          val_ecm_stats_init, 
                                                                                          flag_name_ecm_stats, 
                                                                                          flag_members_ecm_stats, 
                                                                                          prefix_ecm_stats,
                                                                                          f_tstamp)
        #hdr_ecm_stats_fin, val_ecm_stats_fin = hdr_val_ecm_stats_cleanups_second(hdr_ecm_stats_fin_init, 
        #                                                                         val_ecm_stats_fin_init, 
        #                                                                         val_modded, 
        #                                                                         flag_name_ecm_csfb_stats, 
        #                                                                         flag_members_ecm_csfb_stats, 
        #                                                                         prefix_ecm_csfb_stats, 
        #                                                                         f_tstamp)
        hdr_ecm_stats_fin, val_ecm_stats_fin = hdr_val_ecm_stats_cleanups_second(hdr_ecm_stats_fin_init, 
                                                                                 val_ecm_stats_fin_init, 
                                                                                 flag_name_ecm_csfb_stats, 
                                                                                 flag_members_ecm_csfb_stats, 
                                                                                 prefix_ecm_csfb_stats, 
                                                                                 f_tstamp)
        write_csv_load_to_db(nodename, header_emm_ctl_final, value_emm_ctl_final, 'emmctlmsg', f_tstamp)
        write_csv_load_to_db(nodename, header_esm_ctl_final, value_esm_ctl_final, 'esmctlmsg', f_tstamp)
        write_csv_load_to_db(nodename, header_disc_reason, value_disc_reason, 'discreason', f_tstamp)
        write_csv_load_to_db(nodename, header_gmm_sm_attached_subs_cleaned, val_gmm_sm_attached_subs_cleaned, 'GmmSmAttachedSubs', f_tstamp)
        write_csv_load_to_db(nodename, hdr_gmmsm_actv_fin, val_gmmsm_actv_fin, 'GmmSmActvSubs', f_tstamp)
        write_csv_load_to_db(nodename, hdr_gmmsm_attachreq_fin, val_gmmsm_attachreq_fin, 'GmmSmAttachReq', f_tstamp)
        write_csv_load_to_db(nodename, hdr_gmmsm_attachaccept_fin, val_gmmsm_attachaccept_fin, 'GmmSmAttachAccept', f_tstamp)
        write_csv_load_to_db(nodename, hdr_gmmsm_attach_rej_fail_fin, val_gmmsm_attach_rej_fail_fin, 'GmmSmAttachRejFail', f_tstamp)
        write_csv_load_to_db(nodename, hdr_emm_stats_fin, val_emm_stats_fin, 'EmmStats', f_tstamp)
        write_csv_load_to_db(nodename, hdr_ecm_stats_fin, val_ecm_stats_fin, 'EcmStats', f_tstamp)
        print("Saving raw file as {}".format(sdr_file))

def main():
    node_config_file = '/home/backup/scripts/node.json'
    nodename, ip_addr, user, password = getNodeLogin(node_config_file)
    # Initiate Concurrent Processing for all the nodes
    n = len(nodename)
    db_main_proc = Pool(n).starmap(db_main_task, zip(nodename, ip_addr, user, password))

if __name__ == '__main__':
    main()
