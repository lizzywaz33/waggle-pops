# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 12:06:08 2023

@author: lizzy
"""
import paramiko
import socket
import datetime
import time
import os
import logging
import sage_data_client
import argparse

from waggle.plugin import Plugin

pops_ip_address = '10.31.81.187'
port = 10080
pops_user_name = 'root'
pops_password = 'B1gb0$$!'
vsn = "W09F"

def recursive_list(sftp):
    """
    Returns a recursive directory listing from the current directory in the SFTP Connection.
    This assumes the data are stored as /%y%m%d

    Parameters
    ----------
    sftp: SFTPClient
        The current SFTP Client.

    Returns
    -------
    list: str list
        The list of .nc files available
    """
    file_list = []
    year_months = sftp.listdir()
    for ymd in year_months:
        files = sftp.listdir('/media/usb0/Data/%s' % ymd)
        for fi in files:
            if '.csv' in fi:
                file_list.append('/media/usb0/Data/%s/%s' % (ymd, fi))
                
              
    return sorted(file_list)

def new_file(file_bool):
    if file_bool == 'True':
        # create UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # message to send
        message = 'NewFile=1.000\r'

        try:
            # send UDP packet
            sock.sendto(bytes(message, "utf-8"), (pops_ip_address, port))
        except Exception as e:
                    print("Error sending data")

        # close the socket
        sock.close()
    return

def main(args):
    num_files = int(args.num_files)
    
    with Plugin() as plugin:
        t = paramiko.Transport((pops_ip_address, 22))
        channel = t.connect(username=pops_user_name, password=pops_password)
        sftp = t.open_sftp_client()
        sftp.chdir('/media/usb0/Data/')
        
        file_list = recursive_list(sftp)
        
        # Make sure we have new data off the POPS before we upload 

        for f in file_list:
            if f[0] == ".":
                file_list.remove(f) 
        
        # If we're pulling every file, then load database to make sure we are not uploading redundant data
        file_names = []
        #check the num files
        if num_files > 1:
            print("Accessing beehive...")
            df = sage_data_client.query(
                start="-%dh" % num_files,
                filter={"name": "upload", "vsn": "W08D",
                     "plugin": "registry.sagecontinuum.org/lizzywaz/pops-plugin"}).set_index("timestamp")
            file_names = df['meta.filename'].values
        counter = 0    
        for fi in file_list[-num_files:]:
            base, name = os.path.split(fi)
            try: 
                tmp_size = sftp.stat(file_list[counter])
                dt = datetime.datetime.fromtimestamp(tmp_size.st_mtime)
            except ValueError:
                # File incomplete, skip. Should not happen in one-file mode.
                continue    
            if not int(args.hour) == -1:
                if not int(dt.hour) == int(args.hour):
                    continue 
            if name in file_names:
                #check the beehive for files of the same name. new files will
                #have a new number after x. example HK_20231115x001.csv
                if file_names[counter].timestamp > dt: 
                     continue
            #fix timestamp. 
            timestamp = int(datetime.datetime.timestamp(dt))
            print(timestamp)
            logging.debug("Downloading %s" % fi)
            sftp.get(fi, localpath=name)
            logging.debug("Uploading %s to beehive" % fi)
            print("finished")
            plugin.upload_file(name)
            
            counter += 1
            
            #hard code in a new file start when a new day starts 
            #date_format = "%Y-%m-%d %H:%M:%S" 
            #check = datetime.datetime.strptime(dt, date_format)
            #print(check)
        if (dt.hour + dt.minute + dt.second) == 0: 
            # create UDP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # message to send
            message = 'NewFile=1.000\r'

            try:
                # send UDP packet
                sock.sendto(bytes(message, "utf-8"), (pops_ip_address, port))
            except Exception as e:
                print("Error sending data")

            # close the socket
            sock.close()    
        
        #determine if a new file should be made based on the job script 
        file_bool = args.new_file
        pass_bool = new_file(file_bool)
        print("new file:", pass_bool)
        
        #determine if the memory card needs clearing based on job script
        #delete_bool = args.clear_memory
        #clear_card = clear_POPs_files(delete_bool) 
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog='pops-plugin',
            description='Plugin to transfer POPs data')
    parser.add_argument('-n', '--num-files', default=1,
            help='number of files to transfer (0 to transfer all data)')
    parser.add_argument('-hr', '--hour', default=-1,
            help='Hour of the day to transfer (-1 for all hours)')
    parser.add_argument('-new', '--new_file', default = 'false',
            help='new file is created if true')
    parser.add_argument('-clear', '--clear_memory', default = 'true',
            help='memory of data storage will be cleared if true')
    args = parser.parse_args()
    main(args)
    
#to-do: cleanup the microSD 
#job script re-up and function for creating new files each job script if arg is true 


