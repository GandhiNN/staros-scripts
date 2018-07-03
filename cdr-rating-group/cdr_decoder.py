#!/usr/local/bin/python3

# v5 : refactored 14 Mar 2018
# author : ngakan.gandhi@packet-systems.com
#

import argparse
import subprocess
import glob
import csv
import os
from itertools import groupby
from operator import add
from datetime import datetime

class SmartFormatter(argparse.HelpFormatter):
    def _split_lines(self, text, width):
        if text.startswith('R|'):
            return text[2:].splitlines()
        # This is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)

def argument_list():
    parser = argparse.ArgumentParser(description="StarOS CDR Parser Tool", formatter_class=SmartFormatter)
    parser.add_argument("-m", "--mode", required=True,
                        help="R|choose file read mode -> [SINGLE|MULTIPLE].\n"
                            "Note 1 : Please put '*' at the end of the file name if you choose MULTIPLE.\n"
                            "Note 2 : Please specify the full name if you choose SINGLE.")    
    parser.add_argument("-i", "--inputfile", required=True,
        help="CDR Filename to be decoded")
    parser.add_argument("-g", "--gunzip", action='store_true',
        help="Unarchive the file before decoding")
    args = parser.parse_args()
    mode = str(args.mode)
    inputfile = str(args.inputfile)
    gunzip = str(args.gunzip)
    return mode, inputfile, gunzip

def cdr_decode(mode, inputfile):
    # debug
    #print(inputfile)
    base_dir = "/home/gandhi/cdr/source/"
    decoded_dir = "/home/gandhi/cdr/decoded/"
    cgf = "/home/gandhi/cdr/cgf"
    #cdr_to_be_decoded = sorted(glob.glob(base_dir + inputfile))
    parsed_cdr = decoded_dir + inputfile + '-decoded.txt'
    custom_dict = 'custom6'
    if mode == "MULTIPLE":
        # debug
        #print(inputfile)
        #cdr_to_be_decoded = sorted(glob.glob(base_dir + inputfile))
        cdr_to_be_decoded = sorted(glob.glob(base_dir + inputfile + '*'))
        # debug
        #print(cdr_to_be_decoded)
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
            # Parsing of decoded CDR starts here
            print("DEBUG : Parsing from {}".format(item))
            parseCdr(parsed_cdr)
            cdr_decoded.append(parsed_cdr)
            cdr_decoded_file_name = parsed_cdr.strip().split('/')[-1]
            subprocess.call(["mv", parsed_cdr, decoded_dir])
            subprocess.call(["gzip", decoded_dir+cdr_decoded_file_name])
        return cdr_decoded
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
        # Parsing of decoded CDR starts here
        print("DEBUG : Parsing from {}".format(cdr_to_be_decoded))
        parseCdr(cdr_decoded)
        cdr_decoded_file_name = cdr_decoded.strip().split('/')[-1]
        subprocess.call(["mv", cdr_decoded, decoded_dir])
        subprocess.call(["gzip", decoded_dir+cdr_decoded_file_name])
        return cdr_decoded
    else:
        print("No file found! Exiting...")
        return None

# Create function to write CSV: write_csv
def write_csv(target_file, list_of_lists):
    with open(target_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['cdr_id','record_type','imsi','charging_id','apn',
                         'record_opening_time','cause_for_rec_closing','imei_sv','rat_type','total_vol_gprs',
                         'total_vol_fbc'])
        writer.writerows(list_of_lists)

# Create function to get all CDR entries, save as list : get_cdr
def parseCdr(cdr_decoded):
    csv_output = cdr_decoded + ".csv"
    csv_dir = "/home/gandhi/cdr/csv/"
    with open(cdr_decoded, 'r') as infile:
        cdr_id = str()
        record_type = str()
        imsi = str()
        charging_id = str()
        cause_rec_close = str()
        rec_open_time = str()
        imei = str()
        apn = str()
        vol_gprs_ul_dl = list()
        vol_fbc_ul_dl = list()
        container_list = list()
        vol_gprs_container_list = list()
        vol_fbc_container_list = list()
        cdr_list = list()
        for line in infile:
            if ('CDR #') in line:
                # Hacks to initiate new list per CDR entries
                if container_list: # If container list is populated
                    # Dump content of the list
                    vol_gprs_container_list_int = [int(i) for i in vol_gprs_container_list]
                    vol_fbc_container_list_int = [int(i) for i in vol_fbc_container_list]
                    container_list.append(sum(vol_gprs_container_list_int))
                    container_list.append(sum(vol_fbc_container_list_int))
                    cdr_list.append(container_list)
                # Restart lists anew
                container_list = list()
                vol_gprs_container_list = list()
                vol_fbc_container_list = list()
                cdr_id = line.strip().split()[1]
                container_list.append(cdr_id)
            if ('recordType') in line:
                record_type = line.strip().split()[1]
                container_list.append(record_type)
            if ('servedIMSI') in line:
                imsi = line.strip().split()[1]
                container_list.append(imsi)
            if ('chargingID') in line:
                charging_id = line.strip().split()[1]
                container_list.append(charging_id)                
            if ('accessPointNameNI') in line:
                apn = line.strip().split()[1]
                container_list.append(apn)   
            if ('recordOpeningTime') in line:
                rec_open_time = line.strip().split()[1]
                container_list.append(rec_open_time)                
            if ('causeForRecClosing') in line:
                cause_rec_close = '_'.join(line.strip().split()[1:])
                container_list.append(cause_rec_close)                
            if ('imei-sv') in line:
                imei = line.strip().split()[1]
                container_list.append(imei)
            if ('RATType') in line:
                rat = line.strip().split()[1]
                container_list.append(rat)                   
            if ('dataVolumeGPRS') in line:
                data_vol_gprs = line.strip().split()[1]
                vol_gprs_container_list.append(data_vol_gprs)
            if ('datavolumeFBC') in line:
                data_vol_fbc = line.strip().split()[1]
                vol_fbc_container_list.append(data_vol_fbc)    
    write_csv(csv_output, cdr_list)
    subprocess.call(["mv", csv_output, csv_dir])

def gz_decompress(dir_name, extension):
    os.chdir(dir_name)
    for item in os.listdir(dir_name):
        if item.endswith(extension):
            compressed_file_path = os.path.abspath(item) # get full path of files
            subprocess.call("gunzip " + compressed_file_path, shell=True)

def main():
    start_time = datetime.now()
    dir_name = "/home/gandhi/cdr/source/"
    extension = ".gz"    
    mode, inputfile, gunzip = argument_list()
    # debug
    #print(inputfile)
    print("Executing CDR Decoder...\n")
    print("DEBUG : {}".format(datetime.now()))
    if gunzip:
        print("DEBUG : Decompressing source file...\n")
        gz_decompress(dir_name, extension)
    print("DEBUG : Source File Decompressed!\n")
    print("DEBUG : Decoding File...")
    print("DEBUG : Begin Parsing...\n")
    cdr_file_decoded = cdr_decode(mode, inputfile)
    print(cdr_file_decoded)
    print()
    print('Script Finished Execution in :')
    print(datetime.now() - start_time)

if __name__ == "__main__":
    main()

