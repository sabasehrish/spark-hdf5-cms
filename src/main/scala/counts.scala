import DF._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.Row

object Counts {
  def objcount(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) {
    import spark.implicits._
    val info_df = createInfoDF(sc, spark, dname, chunkSize)
    //var num_evts = info_df.count()
    //println("Num Events:" + num_evts )
    val elec_df = createElectronDF(sc, spark, dname, chunkSize)
    //var num_elecs = elec_df.count() 
    val df = createFilteredElectronDF(spark, elec_df, info_df)
    var num_fe = df.count() 
    //val min_max = df.agg(min("Electron_pt"), max("Electron_pt")).head()
    //val col_min = min_max.getFloat(0)
    //val col_max = min_max.getFloat(1)    
    //println("Num Events:" + num_evts )
    //println("Num Electrons:" + num_elecs)
    println("Num Filtered Electrons: " + num_fe) 
    //println("Min Pt: " + col_min + "Max Pt: " + col_max)
    //myrdd.saveAsTextFile("/global/cscratch1/sd/ssehrish/myfile.txt")
  }
}
