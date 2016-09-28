import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Filters {
  /*Filters for Muon DF*/
  val muonpassUDF = udf {
    (chHadIso: Float, neuHadIso: Float, gammaIso: Float, puIso: Float, pogIDBits: Int, pt: Float) => {
     ((pogIDBits & 1)!=0  && ((chHadIso + Math.max(neuHadIso + gammaIso - 0.5*(puIso), 0)) < (0.12*pt)))

    }
  }

  def filterMuondf(spark: SparkSession, muon_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val fdf = muon_df.withColumn("passfilter", muonpassUDF($"Muon_chHadIso", $"Muon_neuHadIso", $"Muon_gammaIso", $"Muon_puIso", $"Muon_pogIDBits", $"Muon_pt"))
    fdf.createOrReplaceTempView("muons")
    val fdf1 = spark.sql("SELECT * FROM muons WHERE Muon_pt >= 10 and Muon_eta > -2.4 and Muon_eta < 2.4 and passfilter")
    fdf1.show()
    fdf1    
  }

  /*Filters for Tau DF*/
  // In the Princeton version, there is a call to passVeto function
  // which is not doing anything in this filter so I havenot implmented 
  // it
  var taupassUDF = udf {
    (hpsDisc: Long, rawIso3Hits: Float) => ((hpsDisc.toInt & 2) != 0) && (rawIso3Hits <= 5)
  }

  def filterTauDF(spark: SparkSession, tau_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val fdf = tau_df.withColumn("passfilter", taupassUDF($"Tau_hpsDisc", $"Tau_rawIso3Hits"))
    fdf.createOrReplaceTempView("taus")
    val fdf1 = spark.sql("SELECT * FROM taus WHERE Tau_pt >= 10 and Tau_eta > -2.3 and Tau_eta < 2.3 and passfilter")
    fdf1.show()
    fdf1
  }
}
