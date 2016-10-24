
import sys
from mpi4py import MPI
import h5py
import math
import numpy as np

filename = '/scratch2/jbk/TTJets_13TeV_amcatnloFXFX_pythia8_2_all.h5'

rank = MPI.COMM_WORLD.rank  # The process ID (integer 0-3 for 4-process run)
nranks = MPI.COMM_WORLD.size

def read_one(var,gk,dk):
    s_time = MPI.Wtime()
    var_size = var.size
    start = var_size/nranks * rank
    end = start + int(math.ceil(float(var_size)/nranks)) - 1
    if rank==nranks-1 : end=var_size
    
    vars = var[start: end]
    totals=np.zeros(1)
    values=np.zeros(1)
    values[0] = np.sum(vars)

    MPI.COMM_WORLD.Reduce(
        values,
        totals,
        op = MPI.SUM,
        root = 0
    )

    if rank==0:
        e_time = MPI.Wtime()
        print gk, dk, totals, (e_time - s_time)

# a few examples
# m_eta = f['/Muon/Muon.eta']
# m_pt = f['/Muon/Muon.pt']
# t_eta = f['/Tau/Tau.eta']
# t_pt = f['/Tau/Tau.pt']

def sum_of_all():
    with h5py.File(filename, 'r', driver='mpio', comm=MPI.COMM_WORLD) as f:
        for gk in f.keys():
            g = f['/'+gk]
            for dk in g.keys():
                d = g[dk]
                # print gk, dk, read_one(d)
                read_one(d,gk,dk)

def simple_filter():
    with h5py.File(filename, 'r', driver='mpio', comm=MPI.COMM_WORLD) as f:
        # fdf = filterMuonDF(spark, 1, muon_df)

        res = pt>=10 && eta>-2.4 && eta<2.4 && ((kPOGIDBits & kPOG) && ((chHadIso+np.maximum(neuHadIso+gammaIso - .5*puIso,0)) < .12 * pt))

        
