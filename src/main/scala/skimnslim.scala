/*Skim and Slim*/
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkFiles

import java.io.File

import scala.util.control.Exception._
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import ncsa.hdf.hdf5lib.H5
import ncsa.hdf.hdf5lib.HDF5Constants

object skimslim {

  private def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
 case class PartitionInfo(fname: String, begin: Long, end: Long)

  private def getPartitionInfo(dname: String, ds_name: String): Array[PartitionInfo] = {
    val defaultchunk = 100000
    val files = getListOfFiles(dname)
    val arrayBuf = ArrayBuffer[PartitionInfo]()
    for (file <- files) {
       val file_id = H5.H5Fopen(dname+file.getName, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT)
       val dsetid = H5.H5Dopen(file_id, ds_name, HDF5Constants.H5P_DEFAULT)
       val sz = Array[Long](0)
       val dspaceid = H5.H5Dget_space(dsetid)
       H5.H5Sget_simple_extent_dims(dspaceid, sz, null)
       val arrlen = sz(0).toInt
       if (arrlen < defaultchunk) {
         arrayBuf+=PartitionInfo(file.getName, 0, arrlen)
       } else {
       val numchunks = arrlen/defaultchunk
       var sindex = 0
       var eindex = defaultchunk
       //println("Array Length: "+arrlen+ ", "+ numchunks)
       for (j <- 0 to numchunks-1) {
         //println(file.getName+", " + sindex + ", " + (eindex-1))
         arrayBuf+=PartitionInfo(file.getName, sindex, (eindex-1))
         sindex = eindex
         eindex = eindex + defaultchunk
       }
       eindex = eindex-defaultchunk+arrlen%defaultchunk
     //  println(file.getName+", " + sindex + ", "+ eindex)
       arrayBuf+=PartitionInfo(file.getName, sindex, (eindex-1))
       }
    }
    return arrayBuf.toArray
  }

/*  def filterMuondf(sqlC: SQLContext, muons: DataFrame) : DataFrame = {
    import sqlC.implicits._
    muons.registerTempTable("muons")
    val sqlDF = sqlC.sql("SELECT Muon_runNum, Muon_lumisec, Muon_evtNum FROM muons WHERE Muon_pt >= 10 and Muon_eta < 2.4")
    sqlDF.show()
    sqlDF
//    return muons.filter($"Muon_pt" >= 10 && $"Muon_eta" < 2.4)  
  }
*/

  def filterTaudf(taus: DataFrame) : DataFrame = {
    return taus.filter("(Tau_hpsDisc & 2) != 0").filter("Tau_rawIso3Hits <= 5")
  }

  /*Reading Dataset from HDF5 files*/
  def readDataset[T:ClassTag](dsetid: Int, datatype: Int, begin: Long, end: Long) : Array[T] = {
      val result = Array.ofDim[T]((end-begin).toInt)
      val memtype_id = H5.H5Tcopy(datatype)
     // H5.H5Tset_size(memtype_id, sdim)
      var offset = Array[Long](begin)
      var stride = Array[Long](1)
      var blocksize = Array[Long](1)
      var count = Array[Long](end-begin)
      val filespace_id = H5.H5Dget_space(dsetid)
      H5.H5Sselect_hyperslab(filespace_id, HDF5Constants.H5S_SELECT_SET,
                            offset, stride, count, null)


      val memspace_id = H5.H5Screate_simple(1, count, null)
       H5.H5Dread(dsetid, datatype,
                                memspace_id, filespace_id, HDF5Constants.H5P_DEFAULT,
                                result)
     // H5.H5Dread(dsetid, datatype,  HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
       //            HDF5Constants.H5P_DEFAULT, result)
      return result
   }

  def transposeArrayOfRow(arr: Array[Row], gname: String) : Array[Row] = {
    var tarr = new Array[Row](arr(0).length)
    //println(tarr.getClass)
    gname match {
      case "/Muon/" => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getFloat(i), arr(1).getFloat(i), arr(2).getFloat(i), arr(3).getInt(i), arr(4).getInt(i), arr(5).getInt(i),arr(6).getFloat(i), arr(7).getFloat(i), arr(8).getFloat(i), arr(9).getInt(i))
                      }
      }
      case "/Tau/"  => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getFloat(i), arr(1).getFloat(i), arr(2).getFloat(i), arr(3).getInt(i), arr(4).getInt(i), arr(5).getInt(i),arr(6).getLong(i), arr(7).getFloat(i))
                      }
}
      case "/Info/"  => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getInt(i), arr(4).getFloat(i), arr(5).getFloat(i), arr(6).getFloat(i), arr(7).getFloat(i))

                      }
      } //end of case for Info
      case "/GenEvtInfo/"  => {  for (i <- 0 to (arr(0).length -1)) {
                                   tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getFloat(i), arr(4).getFloat(i))
                                 }
                              } // end of case for GenEvtInfo
                } //end of match
    return tarr
  }

  def readDatasets(fname: String, gname: String, dslist: List[String], begin: Long, end: Long): Array[Row] = {
    val file_id = H5.H5Fopen(fname, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT)
    val arrayBuf = ArrayBuffer[Row]()
    if (file_id >= 0) {
    for (dsname <- dslist) {
      val dsetid = H5.H5Dopen(file_id, gname+dsname, HDF5Constants.H5P_DEFAULT)
      if (dsetid >= 0) {
      val datatype = H5.H5Dget_type(dsetid)
      val dclass = H5.H5Tget_class_name(H5.H5Tget_class(datatype))
      val sz = Array[Long](0)
      val dspaceid = H5.H5Dget_space(dsetid)
      H5.H5Sget_simple_extent_dims(dspaceid, sz, null)
      val result = dclass match {
        case "H5T_INTEGER" => if (dsname == "Tau.hpsDisc") {readDataset[Long](dsetid, datatype, begin, end)} else readDataset[Int](dsetid, datatype, begin, end)
        case "H5T_FLOAT" => readDataset[Float](dsetid, datatype, begin, end)
        case "H5T_LONG" => readDataset[Long](dsetid, datatype, begin, end)
      }
      val row =  Row.fromSeq(result.toSeq)
      arrayBuf += row
      //println("assigning row to res: "+ row.length)
      H5.H5Dclose(dsetid)
      println("Closed the dataset: "+ dsname + ", "+ dsetid)
      }
    }
    }
    H5.H5Fclose(file_id)
    val tres = transposeArrayOfRow(arrayBuf.toArray, gname)
    println("finished transpose for: ", gname)
    return tres
  }

  def generateColnames(dsnames: List[String]) : Seq[String] = {
    return dsnames.map(x => x.replace(".","_")).toSeq
  }

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
                          case "/Info/" => rdd.map(x => InfoRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getInt(3), x.getFloat(4), x.getFloat(5), x.getFloat(6), x.getFloat(7))).toDF(colnames: _*)
                          case "/GenEvtInfo/" => rdd.map(x => GenInfoRow(x.getInt(0), x.getInt(1), x.getInt(2), x.getFloat(3), x.getFloat(4))).toDF(colnames: _*)

    }
    return df
  }

  case class MuonRow(eta: Float, pt: Float, phi: Float, evtNum: Int, runNum: Int, lumisec:Int, chHadIso: Float, neuHadIso: Float, puIso: Float, pogIDBits: Int)
  case class TauRow(eta: Float, pt: Float, phi: Float, evtNum: Int, runNum: Int, lumisec:Int, hpsDisc: Long, rawIso3Hits: Float)
  case class InfoRow(runNum: Int, lumiSec: Int, evtNum: Int, metFilterFailBits: Int, pfMET: Float, pfMETphi: Float, puppET: Float, puppETphi: Float)
  case class GenInfoRow(runNum: Int, lumisec: Int, evtNum: Int, weight: Float, scalePDF:Float)


  def main(args: Array[String]) {
    val sc = new SparkContext()
    val spark = SparkSession.builder().appName("Skimming").getOrCreate()

    /*Dir path to operate on, will move out as an input argument*/
    val dname = "/global/cscratch1/sd/ssehrish/h5Files/TTJets_13TeV_amcatnloFXFX_pythia8_2/TTJets_13TeV_amcatnloFXFX_pythia8_2/"
    /*GenEvtInfo Group*/
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val gname = "/GenEvtInfo/"
    val partitionlist = getPartitionInfo(dname, gname+genevtinfo_ds(0))
    val rdd = sc.parallelize(partitionlist, partitionlist.length).flatMap(x => readDatasets(dname+x.fname, gname, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createDataFrame(rdd, spark, dname, "/GenEvtInfo/", genevtinfo_ds)
    genevtinfo_df.cache()
    var t0 = System.nanoTime()
    val sumWeights =  genevtinfo_df.agg(sum("weight")).first.get(0)
    var t1 = System.nanoTime()
    println("Sum of Weights is: " + sumWeights)
    println("It took :" + (t1 - t0) +" ns to calculate the weight")
    println("Num events: " + genevtinfo_df.count())
    
    /*Tau Group*/
    /*val tau_ds: List[String] = List("Tau.eta", "Tau.pt", "Tau.phi", "Tau.evtNum", "Tau.runNum", "Tau.lumisec", "Tau.hpsDisc", "Tau.rawIso3Hits")
    val tau_df = createDataFrame(sc, spark, dname, "/Tau/", tau_ds)
    tau_df.cache()
    println("Num Taus: "+tau_df.count())
    val d = tau_df.groupBy("Tau_evtNum", "Tau_lumisec", "Tau_runNum").max("Tau_pt")
    d.show()
*/
    /*Operations on Muon group*/
  /*  val muon_ds: List[String] = List("Muon.eta", "Muon.pt", "Muon.phi", "Muon.evtNum", "Muon.runNum", "Muon.lumisec", "Muon.chHadIso", "Muon.neuHadIso", "Muon.puIso", "Muon.pogIDBits")
    val muon_df = createDataFrame(sc, spark, dname, "/Muon/", muon_ds)
    muon_df.cache()
    muon_df.show()
    println("Num Muons: " + muon_df.count())
//    filterMuondf(sqlContext, muon_df)
    val c = muon_df.groupBy("Muon_evtNum", "Muon_lumisec", "Muon_runNum").max("Muon_pt")
    c.show()*/
  }
}
