import DF._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.Row

object Histo {
  def createHistogram(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) {
    import spark.implicits._
    val info_df = createInfoDF(sc, spark, dname, chunkSize)
    val elec_df = createElectronDF(sc, spark, dname, chunkSize)
    val df = createFilteredElectronDF(spark, elec_df, info_df)
//val myrdd = sc.parallelize(List(num_evts, num_elecs, num_fe,minValue, maxValue))
    //myrdd.saveAsTextFile("/global/cscratch1/sd/ssehrish/myfile.txt")
    var t0 = System.nanoTime()
    // create a histogram here
    // the following will compute a histogram of the data using bucketCount 
    // number of buckets evenly spaced between the minimum and maximum of the RDD.  
    var hists = df.select("Electron_pt").rdd.map(_.getFloat(0).toDouble).histogram(1000) 
    var t1 = System.nanoTime()
    
    sc.parallelize(List(hists._1)).coalesce(1,true).saveAsTextFile("/global/cscratch1/sd/ssehrish/pt_bckt")
    sc.parallelize(List(hists._2)).coalesce(1,true).saveAsTextFile("/global/cscratch1/sd/ssehrish/pt_count")
    println("It took :" + (t1 - t0) +" ns to histogram pt Electron DF")
 
   //hists._1.foreach(x=>println(x))
    //hists._2.foreach(x=>println(x))

    t0 = System.nanoTime()
    val hists1 = df.select("Electron_eta").rdd.map(_.getFloat(0).toDouble).histogram(5) 
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to histogram eta Electron DF")
    //hists1._1.foreach(x=>println(x))
    //hists1._2.foreach(x=>println(x))
  }
}
