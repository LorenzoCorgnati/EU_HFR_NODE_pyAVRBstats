#!/usr/bin/python3


# Created on Wed Apr 14 11:02:44 2021

# @author: Lorenzo Corgnati
# e-mail: lorenzo.corgnati@sp.ismar.cnr.it


# This application performs basic statistic on the radial bearing angles of 
# radial site time series in order to support the selection of the AVRB_QC threshold range.

import os
import sys
import getopt
import numpy as np
import mysql.connector as sql
from mysql.connector import errorcode
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import glob


####################
# STAT FUNCTIONS
####################

def RUVstats(curStation):
    # This function loads the series of radial .ruv files and calls the functions 
    # for running the statistics on the average radial bearing of the input station
    # of the selected network. Plots are generated for the time series of the 
    # radial vector average bearing and for the overall distribution of the radial
    # vector bearings.
    
    # INPUTS:
    #     curStation: data frame containing information about the stations to be analyzed.
               
    # OUTPUTS:
    #     AVRBerr: error flag.
    
    
    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - RUVstats started.')
    
    # Initialize error flag
    AVRBerr = False
    
    # Retrieve current network_id and station_id
    networkID = curStation['network_id'].to_list()[0]  
    stationID = curStation['station_id'].to_list()[0]  
    
    # Retrieve the AVRB_QC valid range
    avrbRange = [curStation['radial_QC_average_radial_bearing_min'].to_list()[0],
                 curStation['radial_QC_average_radial_bearing_max'].to_list()[0]]
    
    # Retrieve the path containing the ruv files from the current station
    ruvFolder = curStation['radial_input_folder_path'].to_list()[0]  
    
    # List all available ruv files from the current station
    ruvFiles = sorted(glob.glob(ruvFolder + '/**/*.ruv', recursive=True))
    
    # Initialize the series containing the bearing values and the data frame containing 
    # the time series of the average bearings
    avgBear = pd.DataFrame(columns=['Time','Bear'])
    allBear = pd.Series(dtype=float)
        
    # Scan files
    for fileName in ruvFiles:
        try:
            # Load the tabular file content in a data frame
            ruvDF = pd.read_csv(fileName, encoding='iso-8859-15', comment='%', sep='\s+', header=None)
            # Extract the bearing values and put them into the series and data frame
            curBear = ruvDF[ruvDF.columns[14]]
            allBear = allBear.append(curBear)                                          # all current bearings
            # Extract the timestamp and put it into the data frame
            with open(fileName,'r') as fn:
                for line in fn:
                    if '%TimeStamp:' in line:
                        tsStr = line.split(':')[1].strip()
                        break
            avgBear = avgBear.append({'Time': datetime.datetime(*list(map(int,tsStr.split()))), 'Bear': curBear.mean()}, ignore_index=True)      # average bearing
        except pd.errors.EmptyDataError as err:
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - ' + err.args[0] + ' ' + fileName)
        except UnicodeDecodeError as err:
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - ' + err.reason + ' for file ' + fileName)
            
        
    # Plot
    plt.figure(figsize=(20, 15))
    # Time series of radial vector average bearing
    plt.subplot(211)
    plt.plot(avgBear.Time, avgBear.Bear)
    plt.axhline(avrbRange[0], color='red')
    plt.axhline(avrbRange[1], color='red')
    plt.ylabel('Radial vector average bearing')
    plt.xlabel('Time')
    plt.grid()
    plt.title('Time series of radial vector average bearing @ ' + stationID)
    # Overall distribution of radial vector average bearings
    plt.subplot(212)
    plt.hist(allBear, edgecolor='black', bins=int(360/5))
    plt.axvline(avrbRange[0], color='red')
    plt.axvline(avrbRange[1], color='red')
    plt.xlabel('Radial vector bearing')
    plt.ylabel('Occurrences')
    plt.grid()
    plt.title('Distribution of radial vector average bearing @ ' + stationID)
    # Save figure
    plt.savefig(networkID + '/' + networkID +
                '-' + stationID + '_AVRBstats.png')
    plt.close()
    
    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Analysis of radial bearings for '
          + networkID + '-' + stationID + ' successfully performed.')
    
    if(not AVRBerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - RUVstats successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - RUVstats exited with an error.')
        
    return AVRBerr


def NCstats(curStation):
    # This function load the netCDF aggregated dataset into an xarray DataSet 
    # and calls the functions for running the statistics on the average radial bearing 
    # of the input station of the selected network. Plots are generated for the time series 
    # of the radial vector average bearing and for the overall distribution of the radial
    # vector bearings.
    
    # INPUTS:
    #     curStation: data frame containing information about the stations to be analyzed.
               
    # OUTPUTS:
    #     AVRBerr: error flag.
    
    
    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - NCstats started.')
    
    # Initialize error flag
    AVRBerr = False
    
    # Retrieve current network_id and station_id
    networkID = curStation['network_id'].to_list()[0]  
    stationID = curStation['station_id'].to_list()[0]  
    
    # Retrieve the AVRB_QC valid range
    avrbRange = [curStation['radial_QC_average_radial_bearing_min'].to_list()[0],
                 curStation['radial_QC_average_radial_bearing_max'].to_list()[0]]

    # Retrieve the SDC_OpenDAP_data_url for current station data
    OpenDAPdataUrl = curStation['SDC_OpenDAP_data_url'].to_list()[0]

    # Read aggregated radial dataset from THREDDS catalog via OpenDAP
    ncDS = xr.open_dataset(OpenDAPdataUrl, decode_times=True)   
    
    # Extract DRVA variable and evaluate the reverse angles to get the bearings
    bear = (ncDS.DRVA + 180) % 360
        
    # Evalute the mean average radial bearing for each timestamp
    avrbTimeSeries = bear.mean(dim=["BEAR", "RNGE", "DEPTH"])
    # avrbTimeSeries = ncDS.DRVA.mean(dim=["BEAR", "RNGE", "DEPTH"])

    # Plot
    plt.figure(figsize=(20, 15))
    # Time series of radial vector average bearing
    plt.subplot(211)
    plt.plot(avrbTimeSeries.TIME, avrbTimeSeries)
    plt.axhline(avrbRange[0], color='red')
    plt.axhline(avrbRange[1], color='red')
    plt.ylabel('Radial vector average bearing')
    plt.xlabel('Time')
    plt.grid()
    plt.title('Time series of radial vector average bearing @ ' + stationID)
    # Overall distribution of radial vector average bearings
    plt.subplot(212)
    plt.hist(bear.values.ravel(), edgecolor='black', bins=int(360/5))
    # plt.hist(ncDS.DRVA.values.ravel(), edgecolor='black', bins=int(360/5))
    plt.axvline(avrbRange[0], color='red')
    plt.axvline(avrbRange[1], color='red')
    plt.xlabel('Radial vector bearing')
    plt.ylabel('Occurrences')
    plt.grid()
    plt.title('Distribution of radial vector average bearing @ ' + stationID)
    # Save figure
    plt.savefig(networkID + '/' + networkID +
                '-' + stationID + '_AVRBstats.png')
    plt.close()

    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Analysis of radial bearings for '
          + networkID + '-' + stationID + ' successfully performed.')
    
    if(not AVRBerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - NCstats successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - NCstats exited with an error.')
        
    return AVRBerr


####################
# MAIN DEFINITION
####################

def main(argv):
    # Define variables for storing arguments
    networkID = ''
    fileType = ''
   
    # Set the argument structure
    try:
        opts, args = getopt.getopt(argv,"n:t:h",["network=","type=", "help"])
    except getopt.GetoptError:
        print('Usage: pyAVRBstats.py -n <network ID> -t <radial file type (nc or ruv)>')
        sys.exit(2)
        
    if not argv:
        print('Usage: pyAVRBstats.py -n <network ID> -t <radial file type (nc or ruv)>')
        sys.exit(2)
        
    for opt, arg in opts:
        if opt == '-h':
            print('pyAVRBstats.py -n <network ID> -t <radial file type (nc or ruv)>')
            sys.exit()
        elif opt in ("-n", "--network"):
            networkID = arg
        elif opt in ("-t", "--type"):
            fileType = arg

    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - pyAVRBstats started.')
        
####################
# SETUP
####################
    
    # Initialize error flag
    AVRBerr = False
    
    # Set parameter for Mysql database connection
    sqlConfig = {
      'user': 'HFR_lorenzo',
      'password': 'xWeLXHFQfvpBmDYO',
      'host': '150.145.136.8',
      'database': 'HFR_node_db',
    }
    
####################    
# NETWORK DATA COLLECTION
####################
    
    # Connect to database
    try:
        cnx = sql.connect(**sqlConfig)
    except sql.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            AVRBerr = True
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - pyAVRBstats exited with an error.')
            sys.exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            AVRBerr = True
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - pyAVRBstats exited with an error.')
            sys.exit()
        else:
            AVRBerr = True
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - pyAVRBstats exited with an error.')
            sys.exit()
    else:
        # Set and execute the query
        networkSelectQuery = 'SELECT * FROM network_tb WHERE network_id=\'' + networkID + '\''
        networkData = pd.read_sql(networkSelectQuery, con=cnx)
        numNetworks = networkData.shape[0]
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Network data successfully fetched from database.')
        
####################    
# STATION DATA COLLECTION
####################                
        
    # Scan networks 
    for netIDX in range(numNetworks):
        # Set and execute the query for getting station data
        stationSelectQuery = 'SELECT * FROM station_tb WHERE network_id=\'' + networkID + '\''
        stationData = pd.read_sql(stationSelectQuery, con=cnx)
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Station data for ' + networkID + ' network successfully fetched from database.')
    
####################    
# ANALYSIS
####################

        # Create the folder for the resulting plots
        if not(os.path.isdir(networkID)):
            try:
                os.mkdir(networkID)
            except OSError:
                print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Creation of ' + networkID + ' folder failed.')
            else:
                print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Successfully created folder ' + networkID + '.')
                
        # Retrieve the number of radial stations
        numStations = stationData.shape[0]
        
        # Scan stations
        for staIDX in range(numStations):
            # Retrieve current station_id
            stationID = stationData['station_id'].to_list()[staIDX]   
            
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Analysing ' 
              + networkID + '-' + stationID + ' ...')

            if fileType=='nc':
                try:
                    AVRBerr = NCstats(stationData.iloc[[staIDX]])
                except OSError:
                    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - ERROR processing ' + 
                          OSError.filename + ' -> ' + OSError.strerror + '.')
                    AVRBerr = True
            elif fileType == 'ruv':
                try:
                    AVRBerr = RUVstats(stationData.iloc[[staIDX]])
                except OSError:
                    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - ERROR processing ' + 
                          OSError.filename + ' -> ' + OSError.strerror + '.')
                    AVRBerr = True

    # Close connection to database
    cnx.close()
    
####################
    
    if(not AVRBerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - pyAVRBstats successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - pyAVRBstats exited with an error.')
            
####################


#####################################
# SCRIPT LAUNCHER
#####################################    
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
    
