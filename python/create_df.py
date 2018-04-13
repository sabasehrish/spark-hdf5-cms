import sys
from mpi4py import MPI
import h5py
import math
import numpy as np
import glob
import pandas as pd


# example path on our local beegfs 
# '/beegfs-scratch/ssehrish/cms-hdf5/*'
# example paths on Cori scrath, *_st_40 => striped on 40 OSTs 
#'/global/cscratch1/sd/ssehrish/h5out_st_40/*' 
#'/global/cscratch1/sd/ssehrish/h5out_all_st/*'
dirname='/global/cscratch1/sd/ssehrish/h5out_merged/*' 

# Need to revisit how we want to describe column names and paths 
info_ds_path=np.array(('/Info/runNum/', 'Info/lumiSec/', '/Info/evtNum/', '/Info/rhoIso/', '/Info/metFilterFailBits/', '/Info/pfMET/', '/Info/pfMETphi/', '/Info/puppET/', '/Info/puppETphi/'))
info_columns=['runNum', 'lumiSec', 'evtNum', 'rhoIso', 'metFilterFailBits', 'pfMET', 'pfMETphi', 'puppET', 'puppETphi']

elec_ds_path=np.array(('/Electron/Electron.runNum/', '/Electron/Electron.lumisec/', '/Electron/Electron.evtNum/', '/Electron/Electron.eta/', '/Electron/Electron.pt/', '/Electron/Electron.phi/', '/Electron/Electron.chHadIso/', '/Electron/Electron.neuHadIso/', '/Electron/Electron.gammaIso/', '/Electron/Electron.scEta/', '/Electron/Electron.sieie/', '/Electron/Electron.hovere/', '/Electron/Electron.eoverp/', '/Electron/Electron.dEtaIn/', '/Electron/Electron.dPhiIn/', '/Electron/Electron.ecalEnergy/', '/Electron/Electron.d0/', '/Electron/Electron.dz/', '/Electron/Electron.nMissingHits/', '/Electron/Electron.isConv/'))
elec_columns=['Electron.runNum', 'Electron.lumisec', 'Electron.evtNum', 'Electron.eta', 'Electron.pt', 'Electron.phi', 'Electron.chHadIso', 'Electron.neuHadIso', 'Electron.gammaIso', 'Electron.scEta', 'Electron.sieie', 'Electron.hovere', 'Electron.eoverp', 'Electron.dEtaIn', 'Electron.dPhiIn', 'Electron.ecalEnergy', 'Electron.d0', 'Electron.dz', 'Electron.nMissingHits', 'Electron.isConv']

rank = MPI.COMM_WORLD.rank  # The process ID (integer 0-3 for 4-process run)
nranks = MPI.COMM_WORLD.size

# create pandas DataFrame
# input: a list of groups 
# output: pandas DataFrame consisting of column list
def create_df(ds_path_list, column_list):
    fv = np.vectorize(read_ds, otypes=[np.ndarray])
    ds = fv(ds_path_list)
    ds = np.array(ds.tolist()) #.transpose()
    ds.shape = len(ds[0]),20
    df = pd.DataFrame(ds)
    df.columns = column_list
    return ds

# read one data set
# input: HDF5 dataset name
# output: dataset read
def read_ds(dsname):
    ds = []
    h5files=glob.glob(dirname)
    for fnames in h5files:
        with h5py.File(fnames, 'r', driver='mpio', comm=MPI.COMM_WORLD) as f:
             d = f[dsname]
             var_size=d.size
             start = var_size/nranks * rank
             end = start + int(math.ceil(float(var_size)/nranks)) - 1
             if rank==nranks-1 : end=var_size
             ds = np.append(ds, d[start: end])
    return ds

# count number of rows in a dataframe
# input: pandas dataframe
# output: row count  
def count_rows(df):
    local_cnt = len(df) #df.shape[0]
    global_cnt = MPI.COMM_WORLD.reduce (local_cnt, op=MPI.SUM, root=0)
    if rank==0:
        print global_cnt

def test():
    s_time = MPI.Wtime()
    df = create_df(elec_ds_path, elec_columns)
    rows = count_rows(df)
    if rank==0:
         e_time = MPI.Wtime()
         print (e_time - s_time)

test()
            
