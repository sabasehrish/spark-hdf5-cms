import Histo._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object runmain {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val spark = SparkSession.builder().master("local").appName("Skimming").getOrCreate()
    if(args.length != 2){
      println("Argument Error:\n    1) data directory\n    2) chunk size")
    } else {
      val dname = args(0)
      val chunkSize = args(1).toInt	//2nd argument defines chunksize when reading h5 files in getPartitionInfo()
      println("Chunk Size: " + chunkSize)
      createHistogram(sc, spark, dname, chunkSize)
    }
  }
}
