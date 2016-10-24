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
  def createDataFrame(spark: SparkSession, rdd: RDD[Row], gname: String, dsnames: List[String]) : DataFrame = {
    import spark.implicits._
   // val colnames = generateColnames(dsnames)
    val schema = gname match {
                          case "/Muon/" => StructType(List(StructField("Electron_eta", FloatType, false), (pt: Float), phi: Float, evtNum: Int, runNum: Int, lumisec:Int, chHadIso: Float, neuHadIso: Float, puIso: Float, pogIDBits: Int, gammaIso: Float)) 
                          case "/Tau/"  => rdd.map(x => TauRow(x.getFloat(0), x.getFloat(1), x.getFloat(2), x.getInt(3), x.getInt(4), x.getInt(5), x.getLong(6), x.getFloat(7))).toDF(colnames: _*)
                          case "/Photon/"  => rdd.map(x => PhotonRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getFloat(9), x.getFloat(10), x.getFloat(11))).toDF(colnames: _*)
                          case "/Electron/"  => rdd.map(x => ElectronRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getFloat(9), x.getFloat(10), x.getFloat(11), x.getFloat(12), x.getFloat(13), x.getFloat(14), x.getFloat(15), x.getFloat(16), x.getFloat(17), x.getInt(18), x.getByte(19))).toDF(colnames: _*)
                          case "/AK4Puppi/"  => rdd.map(x => JetRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getFloat(9), x.getFloat(10), x.getFloat(11), x.getInt(12), x.getInt(13))).toDF(colnames: _*)
                          case "/CA15Puppi/"  => rdd.map(x => VJetRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getFloat(9), x.getFloat(10), x.getFloat(11), x.getInt(12), x.getInt(13))).toDF(colnames: _*)
                          case "/AddCA15Puppi/"  => rdd.map(x => VAddJetRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getFloat(9), x.getFloat(10))).toDF(colnames: _*)
                          case "/Info/" => {
                            StructType(List(StructField("runNum", IntegerType, false), StructField("lumiSec", IntegerType, false), StructField("evtNum",IntegerType, false), StructField("rhoIso", FloatType, false), StructField("metFilterFailBits", IntegerType, false), StructField("pfMET", FloatType, false), StructField("pfMETphi", FloatType, false), StructField("puppET", FloatType, false), StructField("puppETphi", FloatType, false)))
                          } 
                          case "/GenEvtInfo/" => { 
                            StructType(List(StructField("runNum", IntegerType, false), StructField("lumiSec", IntegerType, false), StructField("evtNum", IntegerType, false), StructField("weight", FloatType, false), StructField("scalePDF", FloatType, false)))
                          } 
    }
    return spark.createDataFrame(rdd, schema)
  }

  case class MuonRow(eta: Float, pt: Float, phi: Float, evtNum: Int, runNum: Int, lumisec:Int, chHadIso: Float, neuHadIso: Float, puIso: Float, pogIDBits: Int, gammaIso: Float)
  case class TauRow(eta: Float, pt: Float, phi: Float, evtNum: Int, runNum: Int, lumisec:Int, hpsDisc: Long, rawIso3Hits: Float)
  case class PhotonRow(runNum: Int, lumisec: Int, evtNum: Int, eta: Float, pt: Float, phi: Float, chHadIso: Float, scEta: Float, neuHadIso: Float, gammaIso: Float, sieie: Float, sthovere: Float)
  // 19 datasets from Electron Branch!!!
  case class ElectronRow(runNum: Int, lumisec: Int, evtNum: Int, eta: Float, pt: Float, phi: Float, chHadIso: Float, neuHadIso: Float, gammaIso: Float, scEta: Float, sieie: Float, hovere: Float, eoverp: Float, dEtaIn: Float, dPhiIn: Float, ecalEnergy: Float, d0: Float, dz: Float, nMissingHits: Int, isConv: Byte)
  case class JetRow(runNum: Int, lumisec: Int, evtNum: Int, eta: Float, pt: Float, phi: Float, chHadFrac: Float, chEmFrac: Float, neuHadFrac: Float, neuEmFrac: Float, mass: Float, csv: Float, nParticles: Int, nCharged: Int)
  case class VJetRow(runNum: Int, lumisec: Int, evtNum: Int, eta: Float, pt: Float, phi: Float, chHadFrac: Float, chEmFrac: Float, neuHadFrac: Float, neuEmFrac: Float,  mass: Float, csv: Float, nParticles: Int, nCharged: Int)
  case class VAddJetRow(runNum: Int, lumisec: Int, evtNum: Int, tau1: Float, tau2: Float, tau3: Float, mass_sd0: Float, sj1_csv: Float, sj2_csv: Float, sj3_csv: Float,  sj4_csv: Float)

 }
