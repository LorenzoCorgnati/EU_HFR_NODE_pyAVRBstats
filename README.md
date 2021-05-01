# EU_HFR_NODE_pyAVRBstats
This application is written in Python3 language and it is designed for High Frequency Radar (HFR) data management according to the European HFR node processing workflow. The application performs basic statistic on the radial bearing angles of High Frequency Radar radial site time series in order to support the selection of the Average Radial Bearing QC test's thresholds. In particular, for each radial site of the input HFR network, plots are generated for the time series of the radial vector average bearing and for the overall distribution of the radial vector bearings.

The application perform statistics both starting from the raw .ruv radial files from Codar SeaSonde systems and from the netCDF files archived in the EU HFR NODE THREDDS catalog.

Usage: pyAVRBstats.py -n <network ID> -t <radial file type (nc or ruv)>
Example: pyAVRBstats.py -n HFR-TirLig -t nc
Example: pyAVRBstats.py -n HFR-EUSKOOS -t ruv

The network ID is defined by the EU HFR NODE. Please refer to the "European standard metadata, data and QC model manual" (http://dx.doi.org/10.25607/OBP-944).

THIS APPLICATION IS DESIGNED FOR PROCESSING DATA IN NETCDF FORMAT ACCORDING TO THE EUROPEAN STANDARD DATA AND METADATA MODEL FOR NRT HFR CURRENT DATA. 

The required dependencies are:
- numpy
- mysql.connector
- pandas
- glob
- matplotlib.pyplot
- xarray


Author: Lorenzo Corgnati

Date: May 1, 2021

E-mail: lorenzo.corgnati@sp.ismar.cnr.it 

