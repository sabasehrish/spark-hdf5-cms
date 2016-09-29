import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object Filters {
  /*Filters for Muon DF*/
  // kPOGLooseMuon = 1, kPOGTightMuon = 2
  val muonpassUDF = udf {
    (kPOG: Int, chHadIso: Float, neuHadIso: Float, gammaIso: Float, puIso: Float, pogIDBits: Int, pt: Float) => {
     ((pogIDBits & kPOG)!=0  && ((chHadIso + Math.max(neuHadIso + gammaIso - 0.5*(puIso), 0)) < (0.12*pt)))
    }
  }

  def filterMuonDF(spark: SparkSession, kPOG: Int, muon_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val fdf = muon_df.withColumn("passfilter", muonpassUDF(lit(kPOG), $"Muon_chHadIso", $"Muon_neuHadIso", $"Muon_gammaIso", $"Muon_puIso", $"Muon_pogIDBits", $"Muon_pt"))
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
    (hpsDisc: Long, rawIso3Hits: Float) => ((hpsDisc.toInt & (1 << 16)) != 0) && (rawIso3Hits <= 5)
  }

  def filterTauDF(spark: SparkSession, tau_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val fdf = tau_df.withColumn("passfilter", taupassUDF($"Tau_hpsDisc", $"Tau_rawIso3Hits"))
    fdf.show()
    fdf.createOrReplaceTempView("taus")
    val fdf1 = spark.sql("SELECT * FROM taus WHERE Tau_pt >= 10 and Tau_eta > -2.3 and Tau_eta < 2.3 and passfilter")
    fdf1.show()
    fdf1
  }

  /*Filters for Jet and VJet*/
  //Fix this filter
  var jetpassUDF = udf {
    (neuHadFrac: Float, neuEmFrac: Float, nParticles: Int, eta: Float, chHadFrac: Float, nCharged: Int, chEmFrac: Float) => {
       (neuHadFrac >= 0.99) || neuEmFrac >= 0.99 || nParticles <= 1 || ((eta < 2.4 && eta > -2.4) && (chHadFrac == 0 || nCharged == 0 || chEmFrac >= 0.99))
    }
  }
  def filterJetDF(spark: SparkSession, jet_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val fdf = jet_df.withColumn("passfilter", jetpassUDF($"AK4Puppi_neuHadFrac", $"AK4Puppi_neuEmFrac", $"AK4Puppi_nParticles", $"AK4Puppi_eta", $"AK4Puppi_chHadFrac", $"AK4Puppi_nCharged", $"AK4Puppi_chEmFrac"))
    fdf.show()
    fdf.createOrReplaceTempView("Jets")
    val fdf1 = spark.sql("SELECT * FROM Jets WHERE AK4Puppi_pt >=30 and AK4Puppi_eta < 4.5 and AK4Puppi_eta > -4.5 and passfilter")
    fdf1.show()
    fdf1
  }
}
