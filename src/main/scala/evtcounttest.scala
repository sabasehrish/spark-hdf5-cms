import Filters._
import H5Read._
import H5DataFrame._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object EventcountTest {
  def runevtcounttest(sc: SparkContext, spark: SparkSession, dname: String) {
    import spark.implicits._
    val genevtinfo_df = createGenInfoDF(sc, spark, dname)
    genevtinfo_df.persist()
    val counts = new Array[Long](10)
    for (i <- 0 to 10) {
    var t0 = System.nanoTime()
    counts(i) =  genevtinfo_df.count() 
    var t1 = System.nanoTime()
    println("Num of events is: " + counts(i))
    println("It took :" + (t1 - t0) +" ns to count the events")
    }
  }

  def createGenInfoDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
       /*Gen Event Info to calculate sum of weights*/
    val genevtinfo_gn = "/GenEvtInfo/"
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val genevtinfo_pl = getPartitionInfo(dname, genevtinfo_gn+"weight")
    val genevtinfo_rdd = sc.parallelize(genevtinfo_pl, genevtinfo_pl.length).flatMap(x=> readDatasets(dname+x.fname, genevtinfo_gn, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createH5DataFrame(spark, genevtinfo_rdd, genevtinfo_gn)
    genevtinfo_df
  }

}

