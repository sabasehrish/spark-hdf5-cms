########################################
##
##
##	TODO:
##	  Investigate using 'pysql = lambda q: pdsql.sqldf(q, {'df':df})' with different parameters
##	  I got different timeings when passing it {'df':df} versus local() and global. But regardless
##	  of which param was passed, the lambda functions were always faster.
##
########################################

import h5py
import numpy as np
import pandas as pd
import pandasql as pdsql
import time
import math

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

def muonpassUDF(e, kPOG):
    return ((int(e.Muon_pogIDBits) & kPOG)!=0  and ((e.Muon_chHadIso + max(e.Muon_neuHadIso + e.Muon_gammaIso - 0.5*(e.Muon_puIso), 0)) < (0.12*e.Muon_pt)))

def taupassUDF(e):
    return ((int(e.Tau_hpsDisc) & (1 << 16)) != 0) and (e.Tau_rawIso3Hits <= 5)

def photonpassUDF(e):
    chiso = max((e.Photon_chHadIso - e.rhoarea0), 0.0)
    neuiso = max((e.Photon_neuHadIso - e.rhoarea1), 0.0)
    phoiso = max((e.Photon_gammaIso - e.rhoarea2), 0.0)

    if (e.Photon_sthovere <= 0.05): return True
    return (abs(e.Photon_scEta) <= 1.479 and ((e.Photon_sieie    <= 0.0103) or (chiso <= 2.44) or (neuiso <= (2.57 + math.exp(0.0044*e.Photon_pt*0.5809))) or
                                    (phoiso <= (1.92 + 0.0043 * e.Photon_pt)))) or (abs(e.Photon_scEta) > 1.479 and  ((e.Photon_sieie    <= 0.0277) or
                                    (chiso <= 1.84) or
                                    (neuiso <= (4.00 + math.exp(0.0040*e.Photon_pt*0.9402))) or
                                    (phoiso <= (2.15+0.0041*e.Photon_pt))))

def EffArea(eta, eta_range, eff_area):
    idx = np.searchsorted(eta_range,eta)
    idx = idx-1 if idx > 0 else 0
    return eff_area[idx]

def rhoeffareaPhoUDF(e, eta_range, eff_area):
    return e.rhoIso *EffArea(e.Photon_scEta, eta_range, eff_area)

def jetpassUDF(e):
    return (e.AK4Puppi_neuHadFrac >= 0.99) or     (e.AK4Puppi_neuEmFrac >= 0.99)   or   (e.AK4Puppi_nParticles <= 1 )    or ((e.AK4Puppi_eta < 2.4 and e.AK4Puppi_eta > -2.4) and (e.AK4Puppi_chHadFrac == 0 or e.AK4Puppi_nCharged == 0 or e.AK4Puppi_chEmFrac >= 0.99))

def vjetpassUDF(e):
    return (e.CA15Puppi_neuHadFrac >= 0.99) or (e.CA15Puppi_neuEmFrac >= 0.99) or (e.CA15Puppi_nParticles <= 1 ) or ((e.CA15Puppi_eta < 2.4 and e.CA15Puppi_eta > -2.4) and (e.CA15Puppi_chHadFrac == 0 or e.CA15Puppi_nCharged == 0 or e.CA15Puppi_chEmFrac >= 0.99))

##########################################################################################

########################
##
##  Lambda version of FilterElectronDF
##
##	SQL time:    9.1778857708
##	Lambda time: 2.79734706879
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
def filterElectronDFSQL(dfS):
    #print(locals())
    pysql = lambda q: pdsql.sqldf(q, {'dfS':dfS})
    dfS = pysql("SELECT *,rhoIso * (CASE WHEN Electron_eta < 0.8 THEN 0.1752 WHEN Electron_eta < 1.3 THEN 0.1862 WHEN Electron_eta < 2.0 THEN 0.1411 WHEN Electron_eta < 2.2 THEN 0.1534 WHEN Electron_eta < 2.3 THEN 0.1903 WHEN Electron_eta < 2.4 THEN 0.2243 WHEN Electron_eta >= 2.4 THEN 0.2687 END) AS rhoEffarea FROM dfS")
    dfS = pysql("SELECT *, CASE WHEN (Electron_isConv=1) THEN 0 ELSE (((ABS(Electron_scEta) < 1.479) AND (((Electron_chHadIso + (CASE WHEN CAST(0.0 AS DOUBLE) < (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) THEN (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) ELSE 0 END)) < (0.126 * Electron_pt)) OR (ABS(Electron_dEtaIn) < 0.01520) OR (ABS(Electron_dPhiIn) < 0.21600) OR (Electron_sieie < 0.01140))) OR ((ABS(Electron_scEta) >= 1.479) AND (((Electron_chHadIso + (CASE WHEN CAST(0.0 AS DOUBLE) < (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) THEN (Electron_gammaIso + Electron_neuHadIso - rhoEffarea) ELSE 0 END)) < (0.144 * Electron_pt)) OR (ABS(Electron_dEtaIn) < 0.01130) OR (ABS(Electron_dPhiIn) < 0.23700) OR (Electron_sieie < 0.03520)))) END AS passfilter1 FROM dfS")
    dfS = pysql("SELECT *, (((ABS(Electron_scEta) < 1.479) AND ( (Electron_hovere < 0.18100) OR (ABS(1.0 - Electron_eoverp) < (0.20700* Electron_ecalEnergy)) OR (ABS(Electron_d0) < 0.05640) OR (ABS(Electron_dz) < 0.47200) OR (Electron_nMissingHits <=  2))) OR ((ABS(Electron_scEta) >= 1.479) AND ( (Electron_hovere < 0.11600) OR (ABS(1.0 - Electron_eoverp) < (0.17400* Electron_ecalEnergy)) OR (ABS(Electron_d0) < 0.22200) OR (ABS(Electron_dz) < 0.92100) OR (Electron_nMissingHits <=  3)))) AS passfilter2 FROM dfS")
    dfS = pysql("SELECT * FROM dfS WHERE Electron_pt >= 10 and Electron_eta < 2.5 and Electron_eta > -2.5 and passfilter1 and passfilter2")
    return dfS

#########################
##
##  Python Versions filterMuonDF()  
##
##	SQL time:    4.27902197838
##	Lambda time: 0.76926112175
##
##########################
def filterMuonDF(df, kPOG):
    df = df[df.apply(lambda x: muonpassUDF(x,kPOG),1)]
    return df[(df.Muon_pt >= 10) & (abs(df.Muon_eta) < 2.4)]
def filterMuonDFsql(df, kPOG):
    pysql = lambda q: pdsql.sqldf(q, {'df':df})
    df = pysql("SELECT *, (((Muon_pogIDBits & " + str(kPOG) + ")!=0)  AND ((Muon_chHadIso + (CASE WHEN Muon_neuHadIso + Muon_gammaIso - (0.5* Muon_puIso) < 0 THEN 0 ELSE Muon_neuHadIso + Muon_gammaIso - (0.5* Muon_puIso) END )) < (0.12* Muon_pt))) AS passfilter FROM df")
    return pysql("SELECT * FROM df WHERE Muon_pt >= 10 and Muon_eta > -2.4 and Muon_eta < 2.4 and passfilter")

#########################
##
##  Python Lambda filterTauDF()
##
##	SQL time:    9.37505507469
##	Lambda time: 1.19825291634
##
##########################
def filterTauDF(df):
    df = df[df.apply(lambda x: taupassUDF(x),1)]
    return df[(df.Tau_pt >= 10) & (abs(df.Tau_eta) < 2.3)]
def filterTauDFsql(df):
    pysql = lambda q: pdsql.sqldf(q, {'df':df})
    df = pysql("SELECT *, (((Tau_hpsDisc & 65536) = 65536) AND (Tau_rawIso3Hits <= 5)) AS passfilter FROM df")
    return pysql("SELECT * FROM df WHERE Tau_pt >= 10 and Tau_eta > -2.3 and Tau_eta < 2.3 and passfilter")

#########################
##
##  Python Lambda filterPhotonDF()
##
##########################
def filterPhotonDF(df):
    eta_range = [0.0, 1.0, 1.479, 2.0, 2.2, 2.3, 2.4]
    eff_area_0 = [0.0157, 0.0143, 0.0115, 0.0094, 0.0095, 0.0068, 0.0053]
    eff_area_1 = [0.0143, 0.0210, 0.0147, 0.0082, 0.0124, 0.0186, 0.0320]
    eff_area_2 = [0.0725, 0.0604, 0.0320, 0.0512, 0.0766, 0.0949, 0.1160]

    df['rhoarea0'] = df.apply(lambda x: rhoeffareaPhoUDF(x,eta_range, eff_area_0),1)
    df['rhoarea1'] = df.apply(lambda x: rhoeffareaPhoUDF(x,eta_range, eff_area_1),1)
    df['rhoarea2'] = df.apply(lambda x: rhoeffareaPhoUDF(x,eta_range, eff_area_2),1)

    df = df[df.apply(lambda x: photonpassUDF(x),1)]
    return df[(df.Photon_pt >= 175) & (abs(df.Photon_eta) < 1.4442)]
##########
##
##  NOTICE: filterPhotonDFsql is broken. SQLite3 does not provide an equivilant to GREATEST, which can be worked around. However, it does not provide an equivilant to EXP; so a UDF will be needed.
##
##########
def filterPhotonDFsqlBroken(df):
    pysql = lambda q: pdsql.sqldf(q, {'df':df})
    df = pysql("SELECT *, rhoIso * (CASE WHEN Photon_sceta < 1.0 THEN 0.0157 WHEN Photon_sceta < 1.479 THEN 0.0143 WHEN Photon_sceta < 2.0 THEN 0.0115 WHEN Photon_sceta < 2.2 THEN 0.0094 WHEN Photon_sceta < 2.3 THEN 0.0095 WHEN Photon_sceta < 2.4 THEN 0.0068 WHEN Photon_sceta >= 2.4 THEN 0.0053 END) AS rhoArea0 FROM df")
    df = pysql("SELECT *, rhoIso * (CASE WHEN Photon_sceta < 1.0 THEN 0.0143 WHEN Photon_sceta < 1.479 THEN 0.0210 WHEN Photon_sceta < 2.0 THEN 0.0147 WHEN Photon_sceta < 2.2 THEN 0.0082 WHEN Photon_sceta < 2.3 THEN 0.0124 WHEN Photon_sceta < 2.4 THEN 0.0186 WHEN Photon_sceta >= 2.4 THEN 0.0320 END) AS rhoArea1 FROM df")
    df = pysql("SELECT *, rhoIso * (CASE WHEN Photon_sceta < 1.0 THEN 0.0725 WHEN Photon_sceta < 1.479 THEN 0.0604 WHEN Photon_sceta < 2.0 THEN 0.0320 WHEN Photon_sceta < 2.2 THEN 0.0512 WHEN Photon_sceta < 2.3 THEN 0.0766 WHEN Photon_sceta < 2.4 THEN 0.0949 WHEN Photon_sceta >= 2.4 THEN 0.1160 END) AS rhoArea2 FROM df")
#    df = pysql("SELECT *, CASE WHEN (Photon_sthovere <= 0.05) THEN true ELSE (ABS(Photon_scEta) <= 1.479 AND ((Photon_sieie <= 0.0103) OR ((CASE WHEN(Photon_chHadIso - rhoarea0) < 0.0 THEN 0 ELSE (Photon_chHadIso - rhoarea0) END) <= 2.44) OR ((CASE WHEN(Photon_neuHadIso - rhoarea1) < 0.0 THEN 0 ELSE (Photon_neuHadIso - rhoarea1) END) <= (2.57 + EXP(0.0044* Photon_pt*0.5809))) OR ((CASE WHEN(Photon_gammaIso - rhoarea2) < 0.0 THEN 0 ELSE (Photon_gammaIso - rhoarea2) END) <= (1.92+0.0043* Photon_pt)))) OR (ABS(Photon_scEta) > 1.479 AND  ((Photon_sieie <= 0.0277) OR ((CASE WHEN(Photon_chHadIso - rhoarea0) <0.0 THEN  0.0 ELSE (CASE WHEN(Photon_chHadIso - rhoarea0) END) <= 1.84) OR ((CASE WHEN(Photon_neuHadIso - rhoarea1) < 0.0 THEN 0.0 ELSE (Photon_neuHadIso - rhoarea1) END) <= (4.00 + EXP(0.0040* Photon_pt*0.9402))) OR (GREATEST((Photon_gammaIso - rhoarea2), 0.0) <= (2.15+0.0041* Photon_pt)))) END AS psalter FROM df")
#    df = pysql("SELECT *, CASE WHEN (Photon_sthovere <= 0.05) THEN 1 ELSE (ABS(Photon_scEta) <= 1.479 AND ((Photon_sieie <= 0.0103) OR ((CASE WHEN(Photon_chHadIso - rhoarea0) < 0.0 THEN 0.0 ELSE (Photon_chHadIso - rhoarea0) END) <= 2.44) OR ((CASE WHEN(Photon_neuHadIso - rhoarea1) < 0.0 THEN 0.0 ELSE (Photon_neuHadIso - rhoarea1) END) <= (2.57 + EXP(0.0044* Photon_pt*0.5809))) OR (GREATEST((Photon_gammaIso - rhoarea2), 0.0) <= (1.92+0.0043* Photon_pt)))) OR (ABS(Photon_scEta) > 1.479 AND  ((Photon_sieie <= 0.0277) OR (GREATEST((Photon_chHadIso - rhoarea0), 0.0) <= 1.84) OR (GREATEST((Photon_neuHadIso - rhoarea1), 0.0) <= (4.00 + EXP(0.0040* Photon_pt*0.9402))) OR (GREATEST((Photon_gammaIso - rhoarea2), 0.0) <= (2.15+0.0041* Photon_pt)))) END AS psalter FROM df")
    return pysql("Select * FROM df WHERE Photon_pt >= 175 and Photon_eta < 1.4442 and Photon_eta > -1.4442 and passfilter")

#########################
##
##  Python Lambda filterJetDF
##
##	SQL time:    25.5332009792
##	Lambda time: 7.70425701141
##
##########################
def filterJetDF(df):
    df = df[df.apply(lambda x: jetpassUDF(x),1)]
    return df[(df.AK4Puppi_pt >=30) & (abs(df.AK4Puppi_eta) < 4.5)]
def filterJetDFsql(df):
    pysql = lambda q: pdsql.sqldf(q, {'df':df})
    df = pysql("SELECT *, (AK4Puppi_neuHadFrac >= 0.99) OR AK4Puppi_neuEmFrac >= 0.99 OR AK4Puppi_nParticles <= 1 OR ((AK4Puppi_eta < 2.4 AND AK4Puppi_eta > -2.4) AND (AK4Puppi_chHadFrac == 0 OR AK4Puppi_nCharged == 0 OR AK4Puppi_chEmFrac >= 0.99)) AS passfilter FROM df")
    return pysql("SELECT * FROM df WHERE AK4Puppi_pt >=30 and AK4Puppi_eta < 4.5 and AK4Puppi_eta > -4.5 and passfilter")

#########################
##
##  Python Lambda filterVJetDF()
##
##	SQL time:    0.598764181137
##	Lambda time: 0.218235015869
##
##########################
def filterVJetDF(df):
    df = df[df.apply(lambda x: vjetpassUDF(x),1)]
    return df[(df.CA15Puppi_pt >=150) & (abs(df.CA15Puppi_eta) < 2.5)]
def filterVJetDFsql(df):
    pysql = lambda q: pdsql.sqldf(q, {'df':df})
    df = pysql("SELECT *, (CA15Puppi_neuHadFrac >= 0.99) OR CA15Puppi_neuEmFrac >= 0.99 OR CA15Puppi_nParticles <= 1 OR ((CA15Puppi_eta < 2.4 AND CA15Puppi_eta > -2.4) AND (CA15Puppi_chHadFrac == 0 OR CA15Puppi_nCharged == 0 OR CA15Puppi_chEmFrac >= 0.99)) AS passfilter FROM df")
    return pysql("SELECT * FROM df WHERE CA15Puppi_pt >=150 and CA15Puppi_eta < 2.5 and CA15Puppi_eta > -2.5 and passfilter")

######################################################################################################

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
    elec_df = getDF("/Muon/", f)	# Scala: createElectronDF()
    info_df = getDF("/Info/", f)	# Scala: createInfoDF()
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

    t1 = time.time()
    temp1 = filterMuonDFsql(df1,2)
    t2 = time.time()
    temp2 = filterMuonDF(df2,2)
    t3 = time.time()

    print("SQL size: " + str(temp1.shape))
    print("UDF size: " + str(temp2.shape))
    print(" ")
    print("SQL time: " + str(t2-t1))
    print("UDF time: " + str(t3-t2))

if __name__== "__main__":
    test()
