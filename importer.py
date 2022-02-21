# -*- coding: utf-8 -*-


from base64 import decodebytes
import paramiko
import os
import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
import datetime
import pysftp
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pathlib import Path


os.chdir(Path(__file__).parent)

def sftp_load(backlog = 30):
    
    # Process backlog
    dn = datetime.datetime.now()
    days = datetime.timedelta(backlog)
    od = dn - days
    oldest_file = f'HAL.tsv.{od.strftime("%Y-%m-%d")}'
    
    
    # Get local HAL files
    HAL_list = listdir("HAL")
    # Open HAL log so that ingested files are not downloaded again
    HAL_log = []
    with open('hal_log', 'r') as hl:
        for line in hl:
            HAL_log.append(line.rstrip("\n"))
    # Merge both lists
    HAL_list = HAL_list + HAL_log
    HAL_list.sort()
    #print(HAL_list)
    myHostname = "sftp.tudelft.nl"
    myUsername = "seabroenink"
    myPassword = "Wtrplkpr@5"


    keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABgQCp9NiIYiOj3IfX215Fmzau0o32uG+dKdYuQpcxeLFNHRLTOT+10ROcQvnmmpd5nWu90mG+2r5gAYTWVTr/gnezi89pWap5cgGESJLO6J895wJY63aat984cCTUBTbsanJiQXGCFvVqfpyEnbkVADENwtwojEoxnz1kzwuxjIsiMJIclPsL/AHmiBwwdVDiEpoEp6m9B8DsjLlLbjZifakoHaCcF/bUCiAKSbUfQ89g11OdN4IIMzaFcqpzQJB79cR6O+fl9Snw0xYZ91DcEUyJNe59MvLDAt4SV28XaYem5FKY6Kzsck12FxH9pFkczW0Oj2VEU0CDasvb+1BxH561wQ8guoAGr2UIROEn2LzsLjMvH8FS1s6p65sves2SMN3W5LS3ED6Ikw85y+WKffZ5ijSeMcMitHlHpAKz7z6vmnyLZjB31dr9MRlukh+I+LwXWG4vqfuSdm8aslPkb0yZlDWIcE8VtE8sFrbsf/AeZQG+6v4gu6c1mIk3Pk94cvU="""
    key = paramiko.RSAKey(data=decodebytes(keydata))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.add('sftp.tudelft.nl', 'ssh-rsa', key)

    with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
        print("Connection succesfully stablished ... ")

        # Switch to a remote directory
        sftp.cwd('/staff-bulk/tnw/BT/ebt/G/Sync/HAL/log')

    # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir_attr()

    # Print data
        now = datetime.datetime.now()
        year = now.year
    
        for attr in directory_structure:
            #if attr.filename[:8] == "HAL.tsv":
            #if attr.filename > HAL_list[-1] and attr.filename[:8] == "HAL.tsv.":
            if attr.filename > oldest_file and attr.filename[:8] == "HAL.tsv." and attr.filename not in HAL_list:
                sftp.get(attr.filename, 'HAL_sftp/' + attr.filename)
                print(f"Downloaded {attr.filename} to HAL_sftp")
                
        
        sftp.get('HAL.tsv', 'HAL_sftp/HAL.tsv')
        print(f"Downloaded HAL.tsv (current day) to HAL_sftp")
            # Get the Gasstore data
        sftp.cwd('/staff-bulk/tnw/BT/ebt/G/Sync/Gasstore/' + str(year))
        for reactor in range(1,9):
            try:
                # Get remote files
                directory_structure = sftp.listdir_attr('R0'+str(reactor)+'/analysis')
                # Get local files
                gasdata = listdir('Gasstore/'+str(year)+'/R0' + str(reactor))
                # Get imported files from log
                gaslog = []
                with open('Gasstore/'+str(year)+'/R0'+str(reactor)+'_log', 'r') as gl:
                    for line in gl:
                        gaslog.append(line.rstrip('\n'))
                gasdata = gasdata+gaslog
                gasdata.sort()
                #print(gasdata)
                for attr in directory_structure:
                    try:
                        if attr.filename[:2].isnumeric():
                            check = False
                            if not gasdata:
                                check = True
                                print('empty gasdata')
                            else:
                                check = attr.filename not in gasdata
                            if int(attr.filename[:2]) > 0 and check: # select the month (limiter) and disregard today
                                sftp.get('R0'+str(reactor)+'/analysis/' + attr.filename, 'Gasstore/' + str(year) + '/R0'+ str(reactor) + '/' + attr.filename)
                                print('Downloaded ' + attr.filename + ' to ' + 'Gasstore/' +str(year)+ '/R0'+ str(reactor))
    
                    except:
                        print('file not legit')
            except:
                print('Reactor has no data this year')

    HAL_sftp_list = listdir("HAL_sftp")

    # Strip HAL files, make them less bulky (skip datapoints)
    for s in HAL_sftp_list:
        df = pd.read_csv("HAL_sftp"+"/"+s, sep="\t", index_col=0, parse_dates=False)
        df[[ 0 == int(row[-2:]) % 10 for row in df.index.to_list() ]].to_csv("HAL"+"/"+s, sep="\t")
        #df.to_csv("HAL"+"/"+s, sep="\t")
        os.remove("HAL_sftp/"+s)
        print('converted', s)
        
def upload_DF(df, bucket, measurement):
    token = "ErSLd3zoacZLSNCzdutdLfK0gRmtiAvpgGh5W4qwf6IsFHecteE_8Ma9EDbDE0mk5eg3Zm4ZYKYFZTXRC0XYww=="
    org = "TU Delft"

    #client = InfluxDBClient(url="http://lab.stefbroenink.nl:8086", token=token)
    
    """
    Ingest DataFrame
    """

    startTime = datetime.datetime.now()
    url="http://lab.stefbroenink.nl:8086"
    
    with InfluxDBClient(url=url, token=token, org=org, debug=False) as client:
    
        """
        Use batching API
        """
        with client.write_api() as write_api:
            write_api.write(bucket=bucket, record=df,
                            data_frame_tag_columns=df.index.to_list(),
                            data_frame_measurement_name=measurement)
    
    print(f'Import finished in: {datetime.datetime.now() - startTime}')
    print()

    
def ingest_HAL(HAL_file):
    colnames = [ str(i) for i in range(1,39)]
    df = pd.read_csv('HAL/'+HAL_file, sep='\t', index_col=0, names=colnames)
    x = pd.to_datetime(df.index, dayfirst=True)
    # iterate all reactors
    for r in range(1,9):
        dfr = pd.DataFrame(data=dict(
            x = x,
            do = df.iloc[:,(r - 1)].values,
            ph = df.iloc[:,(r + 7)].values,
            acid = df.iloc[:,(14 + r * 2)].values,
            base = df.iloc[:,(15 + r * 2)].values
            ))
        dfr = dfr.set_index('x')
        print(f"Uploading {HAL_file} to R0{str(r)}")
        upload_DF(dfr, 'R0'+str(r), 'HAL')
        
    # Update log and remove HAL file to save space
    if HAL_file != "HAL.tsv":
        with open('hal_log', 'a') as hl:
            hl.write('\n' + HAL_file)
        os.remove('HAL/' + HAL_file )

def ingest_GAS(year, reactor, GAS_file):
    # Year and reactor as strings, reactor in format: R0x
    try:
        df = pd.read_csv(f'Gasstore/{year}/{reactor}/{GAS_file}', index_col=0, dayfirst=False, parse_dates=True)
        print(f'Uploading {year}/{GAS_file} to {reactor}')
        upload_DF(df, reactor, 'Mass spec')
    except:
        print(f'Error processing {year}/{reactor}/{GAS_file}. File might be empty')

    # Don't log if it is the current day, otherwise you will miss tons of data
    dn = datetime.datetime.now()
    if GAS_file[:4] != dn.strftime('%m') + dn.strftime('%d'):
        with open(f'Gasstore/{year}/{reactor}_log', 'a') as gl:
            gl.write('\n' + GAS_file)
    # Do delete it, so it is downloaded again
    os.remove(f'Gasstore/{year}/{reactor}/{GAS_file}')
    
def process_files():
    HAL_files = listdir('HAL')
    for HF in HAL_files:
        print(f'Processing {HF}')
        ingest_HAL(HF)
        
    for r in range(1,9):
        reactor = f'R0{str(r)}'
        year = datetime.datetime.now().strftime("%Y")
        GAS_files = listdir(f"Gasstore/{year}/{reactor}")
        for GF in GAS_files:
            print(f'Processing {year}/{reactor}/{GF}')
            ingest_GAS(year, reactor, GF)

def install():
    year = datetime.datetime.now().strftime('%Y')
    #year="2023"
    for folder in ['HAL', 'HAL_sftp', 'Gasstore']:
        if not os.path.exists(folder):
            os.makedirs(folder)
    if not os.path.exists(f'Gasstore/{year}'):
        os.makedirs(f'Gasstore/{year}')
    for r in range(1,9):
        if not os.path.exists(f'Gasstore/{year}/R0{str(r)}'):
            os.makedirs(f'Gasstore/{year}/R0{str(r)}')
        if not os.path.exists(f'Gasstore/{year}/R0{str(r)}_log'):
            Path(f'Gasstore/{year}/R0{str(r)}_log').touch()
            

if __name__ == "__main__":
    #print("You are running importer.py as __main__")
    install()
    sftp_load(30)
    process_files()
    if not os.path.exists('log'):
        Path('log').touch()
    with open('log', 'a') as l:
        l.write(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')+'\n')
    #TODO
    #Make list, that tracks what has been imported, so don't DL again
    #Import into InfluxDB
    #Logs and pump integration