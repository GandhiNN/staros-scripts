#!/usr/local/bin/python3
#
# v1 : globbing first
# v1a : working draft to handle multiple vol_ul_dl entries
# v2 : handle multiple file input
# author : ngakan.gandhi@packet-systems.com
# v1 : 2 Jun 2017 - init
# v2 : 6 Jun 2017 - added advanced functions (threshold, file sourcing)
# v3 : 12 Jun 2017 - added function to handle SGSNMME

import subprocess
import glob
import csv
import argparse
import sys
import pprint
import pandas as pd
import subprocess
import os
from itertools import groupby
from operator import add

class SmartFormatter(argparse.HelpFormatter):
    def _split_lines(self, text, width):
        if text.startswith('R|'):
            return text[2:].splitlines()
        # This is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)

def argument_list():
    parser = argparse.ArgumentParser(description="StarOS CDR Parser tool", formatter_class=SmartFormatter)
    parser.add_argument("-m", "--mode", required=True,
                        help="R|choose file read mode -> [SINGLE|MULTIPLE].\n"
                            "Note 1 : Please put '*' at the end of the file name if you choose MULTIPLE.\n"
                            "Note 2 : Please specify the full name if you choose SINGLE.")
    parser.add_argument("-i", "--ifile", required=True, 
                        help="Filename (SINGLE mode), or Filename Pattern* (MULTIPLE mode)")
    parser.add_argument("-t", "--threshold", default=0, type=int, 
                        help="Specify the condition for maximum total usage allowed per event/line")
    parser.add_argument("-s", "--subsid", default='all',
                        help="R|Specify the subscriber's IMSI matching pattern.\n"
                             "all = All IMSIs.\n"
                             "local = All local IMSIs (510-11/510-08).\n"
                             "roaming = All Inroaming IMSIs (non 510-11/510-08).\n"
                             "<IMSI_PATTERN> = All subscribers matching the pattern.")
    args = parser.parse_args()
    mode = str(args.mode)
    inputfile = str(args.ifile)
    threshold = args.threshold
    subsid = args.subsid
    return mode, inputfile, threshold, subsid

def cdr_decode(mode, inputfile):
    base_dir = "/home/backup/cdr/source/"
    cdr_to_be_decoded = ''
    cgf = "/home/backup/scripts/cgf"
    custom_dict = ''
    if mode == "MULTIPLE":
        cdr_to_be_decoded = sorted(glob.glob(base_dir + inputfile))
        cdr_decoded = []
        for item in cdr_to_be_decoded:
            if "SGSN" in item:
                custom_dict = 'custom24'
            else:
                custom_dict = 'custom6'
            parsed_cdr = item + "-decoded.txt"
            subprocess.call(cgf + " -parse_file " + item \
                            + " -generic_file_parse -gtpp_dict " + custom_dict + " -file_format " \
                            + "custom6 > " + parsed_cdr, shell=True)
            cdr_decoded.append(parsed_cdr)
        return cdr_decoded # return list of decoded cdrs
    elif mode == "SINGLE":
        cdr_to_be_decoded = base_dir + inputfile
        cdr_decoded = cdr_to_be_decoded + "-decoded.txt"
        if "SGSN" in inputfile:
            custom_dict = 'custom24'
        else:
            custom_dict = 'custom6'
        subprocess.call(cgf + " -parse_file " + cdr_to_be_decoded \
                        + " -generic_file_parse -gtpp_dict " + custom_dict + " -file_format " \
                        + "custom6 > " + cdr_decoded, shell=True)
        return cdr_decoded # return a single cdr file to be decoded
    else:
        print("No file found! Exiting...")
        return None

def parseCdr(cdr_file_decoded_parsed, threshold, subsid):
    if type(cdr_file_decoded_parsed) is str:
        cdr_id = ''
        record_type = ''
        imsi_roamer = ''
        dat_vol_ul = ''
        dat_vol_dl = ''
        dat_vol_tot = ''
        cause_rec_close = ''
        csv_output = cdr_file_decoded_parsed + ".csv"
        item_orig = cdr_file_decoded_parsed.replace("-parse-temp.txt","")
        print("---------------------------------------------------------------------------------")
        print("Processing from file = ", item_orig)
        with open(cdr_file_decoded_parsed, 'r') as infile:
            lines = infile.readlines()
            message_list = []
            for index, line in enumerate(lines):
                if ("CDR #") in line:
                    line_cdr_id = line
                    cdr_id = line_cdr_id.strip().split()[1]
                    message_list.append(cdr_id)
                if ("recordType") in line:
                    line_record_type = line
                    record_type = line_record_type.strip().split()[1]
                    message_list.append(record_type)
                if ("servedIMSI") in line:
                    line_served_imsi = line
                    imsi_roamer = line_served_imsi.strip().split()[1]
                    message_list.append(imsi_roamer)
                if ("causeForRecClosing") in line:
                    line_cause_rec_close = line
                    cause_rec_close = '_'.join(line_cause_rec_close.strip().split()[1:])
                    message_list.append(cause_rec_close)
                if ("dataVolumeGPRSUplink") in line:
                    line_dat_vol_ul = line
                    dat_vol_ul = line_dat_vol_ul.strip().split()[1]
                    message_list.append(dat_vol_ul)
                if ("dataVolumeGPRSDownlink") in line:
                    line_dat_vol_dl = line
                    dat_vol_dl = line_dat_vol_dl.strip().split()[1]
                    message_list.append(dat_vol_dl)
                if("accessPointNameNI") in line:
                    line_dat_apn = line
                    apn_name = line_dat_apn.strip().split()[1]
                    message_list.append(apn_name)                    
            # Create a list of list, split the original list by "#" (the CDR ID)
            g_list = [list(g) for k, g in groupby(message_list, lambda i : '#' in i)]
            #pprint.pprint(g_list)
            message_list_new = [add(*g_list[i:i+2]) for i in range(0,len(g_list),2)]
            message_list_new = [[item.replace('#','') for item in lst] for lst in message_list_new] # remove hash from cdr_id element
            #pprint.pprint(message_list_new)
            # Extract the DL and UL from original list, cast them into integers 
            #  so we can apply arithmetic operation on them
            message_list_usage_tot = [x[4:-1] for x in message_list_new]
            message_list_usage_int = [[int(j) for j in i] for i in message_list_usage_tot]
            message_tot_usage = [sum(i) for i in message_list_usage_int]
            # Unify the list:
            #  cdr_id;record_type;imsi;total_usage
            final_list = [item[:4] for item in message_list_new]
            i = 0
            while (i < len(final_list)):
                final_list[i].append(message_tot_usage[i])
                final_list[i].append(message_list_new[i][-1])
                i = i + 1
            # Write the file into csv
            with open(csv_output, 'a', encoding='utf-8') as f:
                writer = csv.writer(f, lineterminator='\n') # Avoid using ^M as line terminator
                writer.writerow(['cdr_id','record_type','imsi','apn','total_usage_bits','record_closing_cause'])
                writer.writerows(final_list)
            print("Entries with usage more than or equal to (bits): ", threshold)
            print("Entries with subscriber ID criteria matching with: ", subsid)
            print("---------------------------------------------------------------------------------") 
            df = pd.read_csv(csv_output)
            if subsid == "all":
                threshold_passed = df[df['total_usage_bits'] > threshold]
                print(threshold_passed.to_string(index=False))
            elif subsid == "local":
                threshold_passed = df[(df['total_usage_bits'] > threshold) & \
                                      (df['imsi'].astype(str).str.startswith(("51011", "51008")))]
                print(threshold_passed.to_string(index=False))
            elif subsid == "roaming":
                threshold_passed = df[(df['total_usage_bits'] > threshold) & \
                                      (~df['imsi'].astype(str).str.startswith(("51011", "51008")))] # if not startswith
                print(threshold_passed.to_string(index=False))
            else:
                subs_str = str(subsid)
                threshold_passed = df[(df['total_usage_bits'] > threshold) & \
                                      (df['imsi'].astype(str).str.startswith(subs_str))] # if startswith pattern
                print(threshold_passed.to_string(index=False))
    elif type(cdr_file_decoded_parsed) is list:
        for item in cdr_file_decoded_parsed:
            cdr_id = ''
            record_type = ''
            imsi_roamer = ''
            dat_vol_ul = ''
            dat_vol_dl = ''
            dat_vol_tot = ''
            cause_rec_close = ''
            csv_output = cdr_file_decoded_parsed + ".csv"
            item_orig = item.replace("-parse-temp.txt", "")
            print("---------------------------------------------------------------------------------") 
            print("Processing from file = ", item_orig)
            with open(item, 'r') as infile:
                lines = infile.readlines()
                message_list = []
                for index, line in enumerate(lines):
                    if ("CDR #") in line:
                        line_cdr_id = line
                        cdr_id = line_cdr_id.strip().split()[1]
                        message_list.append(cdr_id)
                    if ("recordType") in line:
                        line_record_type = line
                        record_type = line_record_type.strip().split()[1]
                        message_list.append(record_type)
                    if ("servedIMSI") in line:
                        line_served_imsi = line
                        imsi_roamer = line_served_imsi.strip().split()[1]
                        message_list.append(imsi_roamer)
                    if ("causeForRecClosing") in line:
                        line_cause_rec_close = line
                        cause_rec_close = '_'.join(line_cause_rec_close.strip().split()[1:])
                        message_list.append(cause_rec_close)
                    if ("dataVolumeGPRSUplink") in line:
                        line_dat_vol_ul = line
                        dat_vol_ul = line_dat_vol_ul.strip().split()[1]
                        message_list.append(dat_vol_ul)
                    if ("dataVolumeGPRSDownlink") in line:
                        line_dat_vol_dl = line
                        dat_vol_dl = line_dat_vol_dl.strip().split()[1]
                        message_list.append(dat_vol_dl)
                    if("accessPointNameNI") in line:
                        line_dat_apn = line
                        apn_name = line_dat_apn.strip().split()[1]
                        message_list.append(apn_name)                          
                # Create a list of list, split the original list by "#" (the CDR ID)
                g_list = [list(g) for k, g in groupby(message_list, lambda i : '#' in i)]
                message_list_new = [add(*g_list[i:i+2]) for i in range(0,len(g_list),2)]
                message_list_new = [[item.replace('#','') for item in lst] for lst in message_list_new] # remove hash from cdr_id element
                # Extract the DL and UL from original list, cast them into integers 
                #  so we can apply arithmetic operation on them
                message_list_usage_tot = [x[4:-1] for x in message_list_new]
                message_list_usage_int = [[int(j) for j in i] for i in message_list_usage_tot]
                message_tot_usage = [sum(i) for i in message_list_usage_int]
                # Unify the list:
                #  cdr_id;record_type;imsi;total_usage
                print(message_list_new)
                final_list = [item[:4] for item in message_list_new]
                i = 0
                while (i < len(final_list)):
                    final_list[i].append(message_tot_usage[i])
                    final_list[i].append(message_list_new[i][-1])
                    i = i + 1
                # Write the file into csv
                with open(csv_output, 'a', encoding='utf-8') as f:
                    writer = csv.writer(f, lineterminator='\n') # Avoid using ^M as line terminator
                    writer.writerow(['cdr_id','record_type','imsi','apn','total_usage_bits','record_closing_cause'])
                    writer.writerows(final_list)
                print("Entries with usage more than or equal to (bits): ", threshold)
                print("Entries with subscriber ID criteria matching with: ", subsid)
                print("---------------------------------------------------------------------------------")    
                df = pd.read_csv(csv_output)
                if subsid == "all":
                    threshold_passed = df[df['total_usage_bits'] > threshold]
                    print(threshold_passed.to_string(index=False))
                elif subsid == "local":
                    threshold_passed = df[(df['total_usage_bits'] > threshold) & \
                                      (df['imsi'].astype(str).str.startswith(("51011", "51008")))]
                    print(threshold_passed.to_string(index=False))
                elif subsid == "roaming":
                    threshold_passed = df[(df['total_usage_bits'] > threshold) & \
                                      (~df['imsi'].astype(str).str.startswith(("51011", "51008")))] # if not startswith
                    print(threshold_passed.to_string(index=False))
                else:
                    subs_str = str(subsid)
                    threshold_passed = df[(df['total_usage_bits'] > threshold) & \
                                      (df['imsi'].astype(str).str.startswith(subs_str))] # if startswith pattern
                    print(threshold_passed.to_string(index=False))
   
def housekeeping(): # File housekeeping
    subprocess.call('mv /home/backup/cdr/source/*decoded.txt /home/backup/cdr/decoded/', shell=True)
    subprocess.call('mv /home/backup/cdr/source/*parse-temp.txt /home/backup/cdr/temp/', shell=True)
    subprocess.call('mv /home/backup/cdr/source/*.csv /home/backup/cdr/csv/', shell=True)
    
def gz_decompress(dir_name, extension):
    os.chdir(dir_name)
    for item in os.listdir(dir_name):
        if item.endswith(extension):
            compressed_file_path = os.path.abspath(item) # get full path of files
            subprocess.call("gunzip " + compressed_file_path, shell=True)

def main():
    dir_name = "/home/backup/cdr/source/"
    extension = ".gz"
    gz_decompress(dir_name, extension)
    mode, inputfile, threshold, subsid = argument_list()
    cdr_file_decoded = cdr_decode(mode, inputfile)
    print("Executing parser, press Q to QUIT...\n")
    debug = False
    try:
        if type(cdr_file_decoded) is str: 
            cdr_file_decoded_parsed = cdr_file_decoded + "-parse-temp.txt"
            subprocess.call('cat ' + cdr_file_decoded + ' | grep -Ei "#|recordtype|servedimsi|datavolume|causeforrecclosing|accessPointNameNI" > '
                        + cdr_file_decoded_parsed, shell=True)
            parseCdr(cdr_file_decoded_parsed, threshold, subsid)
        elif type(cdr_file_decoded) is list:
            cdr_file_decoded_parsed = [item + "-parse-temp.txt" for item in cdr_file_decoded]
            i = 0
            while (i < len(cdr_file_decoded_parsed)):
                subprocess.call('cat ' + cdr_file_decoded[i] + ' | grep -Ei "#|recordtype|servedimsi|datavolume|causeforrecclosing|accessPointNameNI" > '
                        + cdr_file_decoded_parsed[i], shell=True)
                parseCdr(cdr_file_decoded_parsed[i], threshold, subsid)
                i = i + 1
        housekeeping()
    except BrokenPipeError:
        if debug:
            raise # re-raise the exception, traceback re-printed
        else:
            print("Skipping the execution of the rest of the file...")
        housekeeping()
        
if __name__ == "__main__":
    main()
