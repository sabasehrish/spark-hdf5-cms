/*Skim and Slim*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkFiles

import java.io.File
import java.io._
import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable.ArrayBuffer

import ncsa.hdf.hdf5lib.H5
import ncsa.hdf.hdf5lib.HDF5Constants

object H5Read {
  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    return (new File(directoryName)).listFiles.filter(_.isDirectory).map(_.getName)
  }

  private def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  case class PartitionInfo(fname: String, begin: Long, end: Long) extends Serializable

  def getPartitionInfo(dname: String, ds_name: String): Array[PartitionInfo] = {
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
       for (j <- 0 to numchunks-1) {
         arrayBuf+=PartitionInfo(file.getName, sindex, (eindex-1))
         sindex = eindex
         eindex = eindex + defaultchunk
       }
       eindex = eindex-defaultchunk+arrlen%defaultchunk
       arrayBuf+=PartitionInfo(file.getName, sindex, (eindex-1))
       }
    }
    return arrayBuf.toArray
  }

  /*Reading Dataset from HDF5 files*/
  def readDataset[T:ClassTag](dsetid: Int, datatype: Int, begin: Long, end: Long) : Array[T] = {
      val result = Array.ofDim[T]((end-begin).toInt)
      val memtype_id = H5.H5Tcopy(datatype)
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
    gname match {
      case "/Muon/" => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getFloat(i), arr(1).getFloat(i), arr(2).getFloat(i), arr(3).getInt(i), arr(4).getInt(i), arr(5).getInt(i),arr(6).getFloat(i), arr(7).getFloat(i), arr(8).getFloat(i), arr(9).getInt(i), arr(10).getFloat(i))
                      }
      }
      case "/Tau/"  => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getFloat(i), arr(1).getFloat(i), arr(2).getFloat(i), arr(3).getInt(i), arr(4).getInt(i), arr(5).getInt(i),arr(6).getLong(i), arr(7).getFloat(i))
                      }
      }
      case "/Photon/" => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getFloat(i), arr(4).getFloat(i), arr(5).getFloat(i),arr(6).getFloat(i), arr(7).getFloat(i), arr(8).getFloat(i), arr(9).getFloat(i), arr(10).getFloat(i), arr(11).getFloat(i))
                      }
      }
     case "/Electron/" => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getFloat(i), arr(4).getFloat(i), arr(5).getFloat(i),arr(6).getFloat(i), arr(7).getFloat(i), arr(8).getFloat(i), arr(9).getFloat(i), arr(10).getFloat(i), arr(11).getFloat(i), arr(12).getFloat(i), arr(13).getFloat(i), arr(14).getFloat(i), arr(15).getFloat(i), arr(16).getFloat(i), arr(17).getFloat(i), arr(18).getInt(i), arr(19).getShort(i))
                      }
      }
     case "/AK4Puppi/" => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getFloat(i), arr(4).getFloat(i), arr(5).getFloat(i),arr(6).getFloat(i), arr(7).getFloat(i), arr(8).getFloat(i), arr(9).getFloat(i), arr(10).getInt(i), arr(11).getInt(i))
                      }
      }
     case "/CA15Puppi/" => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getFloat(i), arr(4).getFloat(i), arr(5).getFloat(i),arr(6).getFloat(i), arr(7).getFloat(i), arr(8).getFloat(i), arr(9).getFloat(i), arr(10).getInt(i), arr(11).getInt(i))
                      }
      }

      case "/Info/"  => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getInt(i), arr(4).getFloat(i), arr(5).getFloat(i), arr(6).getFloat(i), arr(7).getFloat(i))

                      }
      }
      case "/GenEvtInfo/"  => {
                      for (i <- 0 to (arr(0).length -1)) {
                        tarr(i) = Row(arr(0).getInt(i), arr(1).getInt(i), arr(2).getInt(i), arr(3).getFloat(i), arr(4).getFloat(i))
                      }
      }

      }
    return tarr
  }
def readDatasets(fname: String, gname: String, dslist: List[String], begin: Long, end: Long): Array[Row] = {
    val file_id = H5.H5Fopen(fname, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT)
    val arrayBuf = ArrayBuffer[Row]()
    if (file_id >= 0) {
    for (dsname <- dslist) {
      val dsetid = H5.H5Dopen(file_id, gname+dsname, HDF5Constants.H5P_DEFAULT)
      if (dsetid >= 0 ) {
      val datatype = H5.H5Dget_type(dsetid)
      val dclass = H5.H5Tget_class_name(H5.H5Tget_class(datatype))
      val sz = Array[Long](0)
      val dspaceid = H5.H5Dget_space(dsetid)
      if (dspaceid < 0 ) println("Dataspace error: " + dsname)
      H5.H5Sget_simple_extent_dims(dspaceid, sz, null)
      val arrlen = sz(0).toInt
      val memtype_id = H5.H5Tcopy(datatype)
      val dsize = H5.H5Tget_size(memtype_id)
      val result = dclass match {
        case "H5T_INTEGER" => dsize match {
                                case 8 => readDataset[Long](dsetid, datatype, begin, end)
                                case 4 => readDataset[Int](dsetid, datatype, begin, end)
                                case 1 => readDataset[Boolean](dsetid, datatype, begin, end)
                              }
        case "H5T_FLOAT" => readDataset[Float](dsetid, datatype, begin, end)
        case "H5T_ENUM" => readDataset[Short](dsetid, datatype, begin, end)
      }
      val row =  Row.fromSeq(result.toSeq)
      arrayBuf += row
      H5.H5Dclose(dsetid)
      }
    }
    H5.H5Fclose(file_id)
    } else println("Cannot open the file")

    val tres = transposeArrayOfRow(arrayBuf.toArray, gname)
    return tres
  }

  def generateColnames(dsnames: List[String]) : Seq[String] = {
    return dsnames.map(x => x.replace(".","_")).toSeq
  }

}
