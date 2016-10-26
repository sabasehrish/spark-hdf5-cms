import Filters._
import H5Read._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object H5DataFrame {

/*Creating Spark DataFrame from HDF5 Datasets */
  def createDataFrame(spark: SparkSession, rdd: RDD[Row], gname: String) : DataFrame = {
    val schema = gname match {
                          case "/Muon/"          => StructType(List(StructField("Muon_eta", FloatType, false), StructField("Muon_pt", FloatType, false), StructField("Muon_phi", FloatType, false), StructField("Muon_evtNum", IntegerType, false), StructField("Muon_runNum", IntegerType, false), StructField("Muon_lumisec", IntegerType, false), StructField("Muon_chHadIso", FloatType, false), StructField("Muon_neuHadIso", FloatType, false), StructField("Muon_puIso", FloatType, false), StructField("Muon_pogIDBits", IntegerType, false), StructField("Muon_gammaIso", FloatType, false)))
 
                          case "/Tau/"           => StructType(List(StructField("Tau_eta", FloatType, false), StructField("Tau_pt", FloatType, false), StructField("Tau_phi", FloatType, false), StructField("Tau_evtNum", IntegerType, false), StructField("Tau_runNum", IntegerType, false), StructField("Tau_lumisec", IntegerType, false), StructField("Tau_hpsDisc", LongType, false), StructField("Tau_rawIso3Hits", FloatType, false)))
 
                          case "/Photon/"        => StructType(List(StructField("Photon_runNum", IntegerType, false), StructField("Photon_lumisec", IntegerType, false), StructField("Photon_evtNum", IntegerType, false), StructField("Photon_eta", FloatType, false), StructField("Photon_pt", FloatType, false), StructField("Photon_phi", FloatType, false), StructField("Photon_chHadIso", FloatType, false), StructField("Photon_scEta", FloatType, false), StructField("Photon_neuHadIso", FloatType, false), StructField("Photon_gammaIso", FloatType, false), StructField("Photon_sieie", FloatType, false), StructField("Photon_sthovere", FloatType, false)))
 
                          case "/Electron/"      => StructType(List(StructField("Electron_runNum", IntegerType, false), StructField("Electron_lumisec", IntegerType, false), StructField("Electron_evtNum", IntegerType, false), StructField("Electron_eta", FloatType, false), StructField("Electron_pt", FloatType, false), StructField("Electron_phi", FloatType, false), StructField("Electron_chHadIso", FloatType, false), StructField("Electron_neuHadIso", FloatType,false), StructField("Electron_gammaIso", FloatType, false), StructField("Electron_scEta", FloatType, false), StructField("Electron_sieie", FloatType, false), StructField("Electron_hovere", FloatType, false), StructField("Electron_eoverp", FloatType, false), StructField("Electron_dEtaIn", FloatType, false), StructField("Electron_dPhiIn", FloatType, false), StructField("Electron_ecalEnergy", FloatType, false), StructField("Electron_d0", FloatType, false), StructField("Electron_dz", FloatType, false), StructField("Electron_nMissingHits", IntegerType, false), StructField("Electron_isConv", ByteType, false)))

                          case "/AK4Puppi/"      => StructType(List(StructField("AK4Puppi_runNum", IntegerType, false), StructField("AK4Puppi_lumisec", IntegerType, false), StructField("AK4Puppi_evtNum", IntegerType, false), StructField("AK4Puppi_eta", FloatType, false), StructField("AK4Puppi_pt", FloatType, false), StructField("AK4Puppi_phi", FloatType, false), StructField("AK4Puppi_chHadFrac", FloatType, false), StructField("AK4Puppi_chEmFrac", FloatType,false), StructField("AK4Puppi_neuHadFrac", FloatType, false), StructField("AK4Puppi_neuEmFrac", FloatType, false), StructField("AK4Puppi_mass", FloatType, false) , StructField("AK4Puppi_csv", FloatType, false), StructField("AK4Puppi_nParticles", IntegerType, false), StructField("AK4Puppi_nCharged", IntegerType, false)))

                          case "/CA15Puppi/"     => StructType(List(StructField("CA15Puppi_runNum", IntegerType, false), StructField("CA15Puppi_lumisec", IntegerType, false), StructField("CA15Puppi_evtNum", IntegerType, false), StructField("CA15Puppi_eta", FloatType, false), StructField("CA15Puppi_pt", FloatType, false), StructField("CA15Puppi_phi", FloatType, false), StructField("CA15Puppi_chHadFrac", FloatType, false), StructField("CA15Puppi_chEmFrac", FloatType,false), StructField("CA15Puppi_neuHadFrac", FloatType, false), StructField("CA15Puppi_neuEmFrac", FloatType, false), StructField("CA15Puppi_mass", FloatType, false) , StructField("CA15Puppi_csv", FloatType, false), StructField("CA15Puppi_nParticles", IntegerType, false), StructField("CA15Puppi_nCharged", IntegerType, false)))

                          case "/AddCA15Puppi/"  => StructType(List(StructField("AddCA15Puppi_runNum", IntegerType, false), StructField("AddCA15Puppi_lumiSec", IntegerType, false), StructField("AddCA15Puppi_evtNum", IntegerType, false), StructField("AddCA15Puppi_tau1", FloatType, false), StructField("AddCA15Puppi_tau2", FloatType, false), StructField("AddCA15Puppi_tau3", FloatType, false), StructField("AddCA15Puppi_mass_sd0", FloatType, false), StructField("AddCA15Puppi_sj1_csv", FloatType, false), StructField("AddCA15Puppi_sj2_csv", FloatType, false), StructField("AddCA15Puppi_sj3_csv", FloatType, false), StructField("AddCA15Puppi_sj4_csv", FloatType, false)))

                          case "/Info/"          => StructType(List(StructField("runNum", IntegerType, false), StructField("lumiSec", IntegerType, false), StructField("evtNum",IntegerType, false), StructField("rhoIso", FloatType, false), StructField("metFilterFailBits", IntegerType, false), StructField("pfMET", FloatType, false), StructField("pfMETphi", FloatType, false), StructField("puppET", FloatType, false), StructField("puppETphi", FloatType, false)))

                          case "/GenEvtInfo/"    => StructType(List(StructField("runNum", IntegerType, false), StructField("lumiSec", IntegerType, false), StructField("evtNum", IntegerType, false), StructField("weight", FloatType, false), StructField("scalePDF", FloatType, false)))
    }
    return spark.createDataFrame(rdd, schema)
  }
 }
