/*Skim and Slim*/
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkFiles

import java.io.File

import scala.util.control.Exception._
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import ncsa.hdf.hdf5lib.H5
import ncsa.hdf.hdf5lib.HDF5Constants
import Filters._
import H5Read._

object skimslim {

  /*Creating Spark DataFrame from HDF5 Datasets */
  def createDataFrame(rdd: RDD[Row], spark: SparkSession, dname: String, gname: String, dsnames: List[String]) : DataFrame = {
    val colnames = generateColnames(dsnames)
    //val files = getListOfFiles(dname)
   // val partitionlist = getPartitionInfo(dname, gname+dsnames(0))
    import spark.implicits._
  //  val rdd = sc.parallelize(partitionlist, partitionlist.length).flatMap(x => readDatasets(dname+x.fname, gname, dsnames, x.begin, x.end))
    var df = gname match {
                          case "/Muon/" => rdd.map(x => MuonRow(x.getFloat(0), x.getFloat(1), x.getFloat(2), x.getInt(3), x.getInt(4), x.getInt(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getInt(9)))
                                              .toDF(colnames: _*)
                          case "/Tau/"  => rdd.map(x => TauRow(x.getFloat(0), x.getFloat(1), x.getFloat(2), x.getInt(3), x.getInt(4), x.getInt(5), x.getLong(6), x.getFloat(7)))
                                              .toDF(colnames: _*)
                          case "/Photon/"  => rdd.map(x => PhotonRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7), x.getFloat(8), x.getFloat(9), x.getFloat(10), x.getFloat(11))).toDF(colnames: _*)
                          case "/Info/" => rdd.map(x => InfoRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getInt(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7))).toDF(colnames: _*)
                          case "/GenEvtInfo/" => rdd.map(x => GenInfoRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4))).toDF(colnames: _*)

    }
    return df
  }

  case class MuonRow(eta: Float, pt: Float, phi: Float, evtNum: Int, runNum: Int, lumisec:Int, chHadIso: Float, neuHadIso: Float, puIso: Float, pogIDBits: Int)
  case class TauRow(eta: Float, pt: Float, phi: Float, evtNum: Int, runNum: Int, lumisec:Int, hpsDisc: Long, rawIso3Hits: Float)
  case class InfoRow(runNum: Int, lumiSec: Int, evtNum: Int, metFilterFailBits: Int, pfMET: Float, pfMETphi: Float, puppET: Float, puppETphi: Float)
  case class GenInfoRow(runNum: Int, lumisec: Int, evtNum: Int, weight: Float, scalePDF:Float)
  case class PhotonRow(runNum: Int, lumisec: Int, evtNum: Int, eta: Float, pt: Float, phi: Float, chHadIso: Float, scEta: Float, neuHadIso: Float, gammaIso: Float, sieie: Float, sthovere: Float)

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val spark = SparkSession.builder().appName("Skimming").getOrCreate()

    if(args.length != 1){
      println("Missing input directory name")
    } else {
    val dname = args(0) 

    /*GenEvtInfo Group*/
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val gname = "/GenEvtInfo/"
    val partitionlist = getPartitionInfo(dname, gname+genevtinfo_ds(0))
    val rdd = sc.parallelize(partitionlist, partitionlist.length).flatMap(x => readDatasets(dname+x.fname, gname, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createDataFrame(rdd, spark, dname, "/GenEvtInfo/", genevtinfo_ds)
    genevtinfo_df.cache()
    println("Num events: " + genevtinfo_df.count())
    var t0 = System.nanoTime()
    val sumWeights =  genevtinfo_df.agg(sum("weight")).first.get(0)
    var t1 = System.nanoTime()
    println("Sum of Weights is: " + sumWeights)
    println("It took :" + (t1 - t0) +" ns to calculate the weight")

     /*Tau DF related operations*/
    val tau_gn = "/Tau/"
    val tau_ds: List[String] = List("Tau.eta", "Tau.pt", "Tau.phi", "Tau.evtNum", "Tau.runNum", "Tau.lumisec", "Tau.hpsDisc", "Tau.rawIso3Hits")
    val tau_pl = getPartitionInfo(dname, tau_gn+"Tau.eta")
    val tau_rdd = sc.parallelize(tau_pl, tau_pl.length).flatMap(x=> readDatasets(dname+x.fname, tau_gn, tau_ds, x.begin, x.end))
    val tau_df = createDataFrame(tau_rdd, spark, dname, tau_gn, tau_ds)
    tau_df.show()
    val tau_fdf = filterTauDF(spark, tau_df)
    val tdf = tau_fdf.groupBy("Tau_evtNum", "Tau_lumisec", "Tau_runNum").max("Tau_pt")
    tdf.show()
    
    /*Photon DF related operations*/
    val pho_gn = "/Photon/"
    val pho_ds: List[String] = List("Photon.runNum", "Photon.lumisec", "Photon.evtNum", "Photon.eta", "Photon.pt", "Photon.phi", "Photon.chHadIso", "Photon.scEta", "Photon.neuHadIso", "Photon.gammaIso", "Photon.sieie", "Photon.sthovere")
    val pho_pl = getPartitionInfo(dname, pho_gn+"Photon.eta")
    val pho_rdd = sc.parallelize(pho_pl, pho_pl.length).flatMap(x=> readDatasets(dname+x.fname, pho_gn, pho_ds, x.begin, x.end))
    val pho_df = createDataFrame(pho_rdd, spark, dname, pho_gn, pho_ds)
    pho_df.show()

    }
  }
}
