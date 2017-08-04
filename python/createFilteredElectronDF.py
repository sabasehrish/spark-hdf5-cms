import h5py
import numpy as np
import pandas as pd
import pandasql as pdsql
import time

###########################################################################################
##
##
##	UDF Functions
##
##
###########################################################################################
def pyUDF(target,rho):
    eta_range = [0.0, 0.8, 1.3, 2.0, 2.2, 2.3, 2.4]
    eff_area = [0.1752, 0.1862, 0.1411, 0.1534, 0.1903, 0.2243, 0.2687]

    idx = np.searchsorted(eta_range,target)
    idx = idx-1 if idx > 0 else 0

    return rho * eff_area[idx]

def elecpassUDF(e):
    iso = e.Electron_chHadIso + max([0.0, (e.Electron_gammaIso + e.Electron_neuHadIso)])    #add rhoaarea back
    if (e.Electron_isConv==1): return False
    return (((abs(e.Electron_scEta) < 1.479) and ((iso                   < (0.126 * e.Electron_pt))         or
                                       (abs(e.Electron_dEtaIn)       < 0.01520) or
                                       (abs(e.Electron_dPhiIn)       < 0.21600)              or
                                       (e.Electron_sieie                  < 0.01140))) or
        ((abs(e.Electron_scEta) >= 1.479) and ((iso                   < (0.144 * e.Electron_pt))         or
                                       (abs(e.Electron_dEtaIn)       < 0.01130) or
                                       (abs(e.Electron_dPhiIn)       < 0.23700)              or
                                       (e.Electron_sieie                  < 0.03520))))

def elecpass2UDF(e):
    return (((abs(e.Electron_scEta) < 1.479) and (
                                       (e.Electron_hovere                 < 0.18100)              or
                                       (abs(1.0 - e.Electron_eoverp) < (0.20700*e.Electron_ecalEnergy)) or
                                       (abs(e.Electron_d0)           < 0.05640)              or
                                       (abs(e.Electron_dz)           < 0.47200)              or
                                       (e.Electron_nMissingHits           <=  2))) or
        ((abs(e.Electron_scEta) >= 1.479) and (
                                       (e.Electron_hovere                 < 0.11600)              or
                                       (abs(1.0 - e.Electron_eoverp) < (0.17400*e.Electron_ecalEnergy)) or
                                       (abs(e.Electron_d0)           < 0.22200)              or
                                       (abs(e.Electron_dz)           < 0.92100)              or
                                       (e.Electron_nMissingHits           <=  3))))
##########################################################################################

########################
##
##  Lambda version of FilterElectronDF
##
#########################
def filterElectronDF(df):
    df['rhoEffarea'] = df.apply(lambda x: pyUDF(x.Electron_eta, x.rhoIso),1)
    df['passfilter1']= df.apply(lambda x: elecpassUDF(x), 1)
    df['passfilter2']= df.apply(lambda x: elecpass2UDF(x),1)
    df = df[df.passfilter1]
    df = df[df.passfilter2]
    df = df[df.Electron_pt >= 10]
    df = df[df.Electron_eta < 2.5]
    df = df[df.Electron_eta > -2.5]
    return df

def filterElectronDF2(df):
    df['rhoEffarea'] = df.apply(lambda x: pyUDF(x.Electron_eta, x.rhoIso),1)
    df = df[df.apply(lambda x: elecpassUDF(x), 1)]
    df = df[df.apply(lambda x: elecpass2UDF(x),1)]
    df = df[(abs(df.Electron_eta) < 2.5) & (df.Electron_pt >= 10)]
    return df

#########################
##
##  SQL version of filterElectronDF
##
########################
def filterElectronDFSQL(df):
    #print(locals())
    pysql = lambda q: pdsql.sqldf(q, {'df':df})
    dfS = pysql("SELECT *,rhoIso * (CASE WHEN Electron_eta < 0.8 THEN 0.1752 WHEN Electron_eta < 1.3 THEN 0.1862 WHEN Electron_eta < 2.0 THEN 0.1411 WHEN Electron_eta < 2.2 THEN 0.1534 WHEN Electron_eta < 2.3 THEN 0.1903 WHEN Electron_eta < 2.4 THEN 0.2243 WHEN Electron_eta >= 2.4 THEN 0.2687 END) AS rhoEffarea FROM df")
    pysql = lambda q: pdsql.sqldf(q, {'dfS':dfS})
    dfS = pysql("SELECT *, CASE WHEN (Electron_isConv=1) THEN 0 ELSE (((ABS(Electron_scEta) < 1.479) AND (((Electron_chHadIso + (CASE WHEN CAST(0.0 AS DOUBLE) < (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) THEN (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) ELSE 0 END)) < (0.126 * Electron_pt)) OR (ABS(Electron_dEtaIn) < 0.01520) OR (ABS(Electron_dPhiIn) < 0.21600) OR (Electron_sieie < 0.01140))) OR ((ABS(Electron_scEta) >= 1.479) AND (((Electron_chHadIso + (CASE WHEN CAST(0.0 AS DOUBLE) < (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) THEN (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) ELSE 0 END)) < (0.144 * Electron_pt)) OR (ABS(Electron_dEtaIn) < 0.01130) OR (ABS(Electron_dPhiIn) < 0.23700) OR (Electron_sieie < 0.03520)))) END AS passfilter1 FROM dfS")
    pysql = lambda q: pdsql.sqldf(q, {'dfS':dfS})
    dfS = pysql("SELECT *, (((ABS(Electron_scEta) < 1.479) AND ( (Electron_hovere < 0.18100) OR (ABS(1.0 - Electron_eoverp) < (0.20700* Electron_ecalEnergy)) OR (ABS(Electron_d0) < 0.05640) OR (ABS(Electron_dz) < 0.47200) OR (Electron_nMissingHits <=  2))) OR ((ABS(Electron_scEta) >= 1.479) AND ( (Electron_hovere < 0.11600) OR (ABS(1.0 - Electron_eoverp) < (0.17400* Electron_ecalEnergy)) OR (ABS(Electron_d0) < 0.22200) OR (ABS(Electron_dz) < 0.92100) OR (Electron_nMissingHits <=  3)))) AS passfilter2 FROM dfS")
    pysql = lambda q: pdsql.sqldf(q, {'dfS':dfS})
    dfS = pysql("SELECT * FROM dfS WHERE Electron_pt >= 10 and Electron_eta < 2.5 and Electron_eta > -2.5 and passfilter1 and passfilter2")
    return dfS

#######################
##
##  Returns dataframe given group
##
######################
def getDF(group, f):
    colStack = np.column_stack([f[group + str(i.replace("_","."))] for i in f[group].keys()])
    return pd.DataFrame(colStack, columns=[i.replace(".","_") for i in f[group].keys()])

###################
##
##  Setup code, create DF
##
###################
def setup():
    f = h5py.File('/Users/abuchan/Documents/programming/data/test.h5', 'r') 
    elec_df = getDF("/Electron/", f)	# Scala: createElectronDF()
    info_df = getDF("/Info/", fi)	# Scala: createInfoDF()
    return elec_df.join(info_df)

################
##
##  Execution code
##
#################
def test():
    #slow way of creating three seperate instances of df
    df1 = setup()
    df2 = setup()
    df3 = setup()

    #Time all the operations
    t1 = time.time()
    temp1 = filterElectronDF(df1)
    t2 = time.time()
    temp2 = filterElectronDF2(df2)
    t3 = time.time()
    temp3 = filterElectronDFSQL(df3)
    t4 = time.time()

    #Results
    print("UDF1: " + str(t2 - t1))
    print("UDF2: " + str(t3 - t2))
    print("SQL:  " + str(t4 - t3))
    print("Sizes:")
    print("UDF1: " + str(temp1.shape))
    print("UDF2: " + str(temp2.shape))
    print("SQL:  " + str(temp3.shape))
