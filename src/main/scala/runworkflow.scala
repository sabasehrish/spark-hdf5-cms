import Histo._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object runmain {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val spark = SparkSession.builder().master("local").appName("Skimming").getOrCreate()
    if(args.length != 1){
      println("Missing input directory name")
    } else {
      val dname = args(0)
      createHistogram(sc, spark, dname)
    }
  }
}
