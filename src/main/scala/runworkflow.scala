import Filters._
import H5Read._
import H5DataFrame._
import SkimWF._
import SoWTest._
import EventcountTest._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object runmain {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val spark = SparkSession.builder().master("local").appName("Skimming").getOrCreate()
    if(args.length != 1){
      println("Missing input directory name")
    } else {
      val dname = args(0)
      //skimworkflow(sc, spark, dname)
      //runSoWtest(sc, spark, dname)
      runevtcounttest(sc, spark, dname)
    }
  }
}
