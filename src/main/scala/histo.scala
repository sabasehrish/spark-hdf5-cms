import DF._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Histo {
  def createHistogram(sc: SparkContext, spark: SparkSession, dname: String) {
    import spark.implicits._
    val info_df = createInfoDF(sc, spark, dname)
    val elec_df = createElectronDF(sc, spark, dname) 
    val df = createFilteredElectronDF(spark, elec_df, info_df) 
    var t0 = System.nanoTime()
    //create a histogram here 
    var hists = df.select("Electron_pt").rdd.map(_.getFloat(0).toDouble).histogram(10) 
    // For vjet pt histogram. Array(194.5977020263672, 300.0, 450.00, 757.8032173156738, 1001.0, 1321.0087326049804, 1884.2142478942872))
      var t1 = System.nanoTime()
      println("It took :" + (t1 - t0) +" ns to histogram pt Electron DF")
      hists._1.foreach(x=>println(x))
      hists._2.foreach(x=>println(x))

     t0 = System.nanoTime()
     val hists1 = df.select("Electron_eta").rdd.map(_.getFloat(0).toDouble).histogram(10) 
     t1 = System.nanoTime()
     println("It took :" + (t1 - t0) +" ns to histogram eta Electron DF")
     hists1._1.foreach(x=>println(x))
     hists1._2.foreach(x=>println(x))
  }
}
