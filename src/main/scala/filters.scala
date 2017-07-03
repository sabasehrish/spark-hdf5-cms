import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection.Searching._

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
    
    //Old UDF function for muonpassUDF
    //val fdf = muon_df.withColumn("passfilter", muonpassUDF(lit(kPOG), $"Muon_chHadIso", $"Muon_neuHadIso", $"Muon_gammaIso", $"Muon_puIso", $"Muon_pogIDBits", $"Muon_pt"))
    
    //SQL replacement for muonpassUDF
    muon_df.createOrReplaceTempView("muons")
    val fdf = spark.sql("SELECT *, (((Muon_pogIDBits & " + kPOG + ")!=0)  AND ((Muon_chHadIso + GREATEST(Muon_neuHadIso + Muon_gammaIso - (0.5* Muon_puIso), 0)) < (0.12* Muon_pt))) AS passfilter FROM muons")


    fdf.createOrReplaceTempView("muons")
    val fdf1 = spark.sql("SELECT * FROM muons WHERE Muon_pt >= 10 and Muon_eta > -2.4 and Muon_eta < 2.4 and passfilter")
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
    
    //Old UDF function for taupassUDF
    //val fdf = tau_df.withColumn("passfilter", taupassUDF($"Tau_hpsDisc", $"Tau_rawIso3Hits"))
    

    //SQL Replacement for udf function
    tau_df.createOrReplaceTempView("taus")
    val fdf = spark.sql("SELECT *, (((Tau_hpsDisc & 65536) = 65536) AND (Tau_rawIso3Hits <= 5)) AS passfilter FROM taus")

    fdf.createOrReplaceTempView("taus")
    val fdf1 = spark.sql("SELECT * FROM taus WHERE Tau_pt >= 10 and Tau_eta > -2.3 and Tau_eta < 2.3 and passfilter")
    fdf1
  }
  /*Utility function used by both photon and electron filters*/
  def EffArea(eta: Float, eta_range: Array[Float], eff_area: Array[Float]) : Float = {
    val searchtype = eta_range.search(eta)
    var index = searchtype.insertionPoint
    if (index == 0) return eff_area(index)
    if (searchtype.toString.startsWith("InsertionPoint")) index = index - 1
    return eff_area(index)
  }

  /*Filters for Photon DF*/
    var photonpassUDF = udf {
    (chHadIso: Float, neuHadIso: Float, gammaIso: Float, scEta: Float, sthovere: Float, sieie: Float, pt: Float, rhoarea0: Float, rhoarea1: Float, rhoarea2: Float) => {
      val chiso = Math.max((chHadIso - rhoarea0), 0.0)
      val neuiso = Math.max((neuHadIso - rhoarea1), 0.0)
      val phoiso = Math.max((gammaIso - rhoarea2), 0.0)

      if (sthovere <= 0.05) true
      (Math.abs(scEta) <= 1.479 && ((sieie    <= 0.0103) ||
                                    (chiso <= 2.44) ||
                                    (neuiso <= (2.57 + Math.exp(0.0044*pt*0.5809))) ||
                                    (phoiso <= (1.92+0.0043*pt)))) ||
      (Math.abs(scEta) > 1.479 &&  ((sieie    <= 0.0277) ||
                                    (chiso <= 1.84) ||
                                    (neuiso <= (4.00 + Math.exp(0.0040*pt*0.9402))) ||
                                    (phoiso <= (2.15+0.0041*pt))))
    }
  }
  
  def rhoeffareaPhoUDF(eta_range: Array[Float], eff_area:Array[Float]) = udf ((rho:Float, eta: Float) => rho*EffArea(eta, eta_range, eff_area))
 
  def filterPhotonDF(spark: SparkSession, photon_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val eta_range = Array(0.0f, 1.0f, 1.479f, 2.0f, 2.2f, 2.3f, 2.4f)
    val eff_area_0 = Array(0.0157f, 0.0143f, 0.0115f, 0.0094f, 0.0095f, 0.0068f, 0.0053f)
    val eff_area_1 = Array(0.0143f, 0.0210f, 0.0147f, 0.0082f, 0.0124f, 0.0186f, 0.0320f)
    val eff_area_2 = Array(0.0725f, 0.0604f, 0.0320f, 0.0512f, 0.0766f, 0.0949f, 0.1160f)
    
    //Old UDF functions for rhoeffareaPhoUDF
    //var fdf = photon_df.withColumn("rhoArea0", rhoeffareaPhoUDF(eta_range, eff_area_0)($"rhoIso", $"Photon_sceta"))
    //fdf = fdf.withColumn("rhoArea1", rhoeffareaPhoUDF(eta_range, eff_area_1)($"rhoIso", $"Photon_sceta"))
    //fdf = fdf.withColumn("rhoArea2", rhoeffareaPhoUDF(eta_range, eff_area_2)($"rhoIso", $"Photon_sceta"))
    

    //SQL replacement for rhoeffarePhoUDF
    //version 1 (broken)
    //spark.sql("SELECT *, (SELECT effArea.col2  FROM (VALUES (0,0.0), (1,1.0), (2,1.479), (3,2.0), (4,2.2), (5,2.3), (6,2.4)) AS etaRange INNER JOIN (VALUES (0,0.0157), (1,0.0143), (2,0.0115), (3,0.0094), (4,0.0095), (5,0.0068), (6,0.0053)) AS effArea ON etaRange.col1 = effArea.col1 WHERE etaRange.col2=Photon_sceta) AS " + colname + " FROM " + tablename).show() // needs outer apply 
    //Too time consuming to make one giant script so values are hardcoded
    photon_df.createOrReplaceTempView("photons")
    var fdf = spark.sql("SELECT *, rhoIso * (CASE WHEN Photon_sceta < 1.0 THEN 0.0157 WHEN Photon_sceta < 1.479 THEN 0.0143 WHEN Photon_sceta < 2.0 THEN 0.0115 WHEN Photon_sceta < 2.2 THEN 0.0094 WHEN Photon_sceta < 2.3 THEN 0.0095 WHEN Photon_sceta < 2.4 THEN 0.0068 WHEN Photon_sceta >= 2.4 THEN 0.0053 END) AS rhoArea0 FROM photons")
 
    fdf.createOrReplaceTempView("photons")
    fdf = spark.sql("SELECT *, rhoIso * (CASE WHEN Photon_sceta < 1.0 THEN 0.0143 WHEN Photon_sceta < 1.479 THEN 0.0210 WHEN Photon_sceta < 2.0 THEN 0.0147 WHEN Photon_sceta < 2.2 THEN 0.0082 WHEN Photon_sceta < 2.3 THEN 0.0124 WHEN Photon_sceta < 2.4 THEN 0.0186 WHEN Photon_sceta >= 2.4 THEN 0.0320 END) AS rhoArea1 FROM photons")

    fdf.createOrReplaceTempView("photons")
    fdf = spark.sql("SELECT *, rhoIso * (CASE WHEN Photon_sceta < 1.0 THEN 0.0725 WHEN Photon_sceta < 1.479 THEN 0.0604 WHEN Photon_sceta < 2.0 THEN 0.0320 WHEN Photon_sceta < 2.2 THEN 0.0512 WHEN Photon_sceta < 2.3 THEN 0.0766 WHEN Photon_sceta < 2.4 THEN 0.0949 WHEN Photon_sceta >= 2.4 THEN 0.1160 END) AS rhoArea2 FROM photons")


    //Old UDF function for photonpassUDF
    //fdf = fdf.withColumn("passfilter", photonpassUDF($"Photon_chHadIso", $"Photon_neuHadIso", $"Photon_gammaIso", $"Photon_scEta", $"Photon_sthovere", $"Photon_sieie", $"Photon_pt", $"rhoArea0", $"rhoArea1", $"rhoArea2"))
    
    //SQL replacement for photonpassUDF
    fdf.createOrReplaceTempView("photons")
    fdf = spark.sql("SELECT *, CASE WHEN (Photon_sthovere <= 0.05) THEN true ELSE (ABS(Photon_scEta) <= 1.479 AND ((Photon_sieie <= 0.0103) OR (GREATEST((Photon_chHadIso - rhoarea0), 0.0) <= 2.44) OR (GREATEST((Photon_neuHadIso - rhoarea1), 0.0) <= (2.57 + EXP(0.0044* Photon_pt*0.5809))) OR (GREATEST((Photon_gammaIso - rhoarea2), 0.0) <= (1.92+0.0043* Photon_pt)))) OR (ABS(Photon_scEta) > 1.479 AND  ((Photon_sieie <= 0.0277) OR (GREATEST((Photon_chHadIso - rhoarea0), 0.0) <= 1.84) OR (GREATEST((Photon_neuHadIso - rhoarea1), 0.0) <= (4.00 + EXP(0.0040* Photon_pt*0.9402))) OR (GREATEST((Photon_gammaIso - rhoarea2), 0.0) <= (2.15+0.0041* Photon_pt)))) END AS psalter FROM photons")

    fdf.createOrReplaceTempView("photons")
    val fdf1 = spark.sql("Select * FROM photons WHERE Photon_pt >= 175 and Photon_eta < 1.4442 and Photon_eta > -1.4442 and passfilter")
    fdf1
  }

  /*Filters for Electron DF*/
  var elecpassUDF = udf {
    (pt: Float, isConv: Short,  chHadIso: Float, gammaIso: Float, neuHadIso: Float, rhoarea: Float, scEta: Float, dEtaIn: Float, dPhiIn: Float, sieie: Float) => {
      val iso = chHadIso + Math.max(0.0, (gammaIso + neuHadIso - rhoarea))
        if (isConv==1) false
        (((Math.abs(scEta) < 1.479) && ((iso                   < (0.126 * pt))         ||
                                       (Math.abs(dEtaIn)       < 0.01520) ||
                                       (Math.abs(dPhiIn)       < 0.21600)              ||
                                       (sieie                  < 0.01140))) ||
        ((Math.abs(scEta) >= 1.479) && ((iso                   < (0.144 * pt))         ||
                                       (Math.abs(dEtaIn)       < 0.01130) ||
                                       (Math.abs(dPhiIn)       < 0.23700)              ||
                                       (sieie                  < 0.03520))))
    }
  }

  var elecpass2UDF = udf {
    (scEta: Float, hovere: Float, eoverp: Float, ecalEnergy: Float, d0: Float, dz: Float, nMissingHits: Int) => {
        (((Math.abs(scEta) < 1.479) && (
                                       (hovere                 < 0.18100)              ||
                                       (Math.abs(1.0 - eoverp) < (0.20700*ecalEnergy)) ||
                                       (Math.abs(d0)           < 0.05640)              ||
                                       (Math.abs(dz)           < 0.47200)              ||
                                       (nMissingHits           <=  2))) ||
        ((Math.abs(scEta) >= 1.479) && (
                                       (hovere                 < 0.11600)              ||
                                       (Math.abs(1.0 - eoverp) < (0.17400*ecalEnergy)) ||
                                       (Math.abs(d0)           < 0.22200)              ||
                                       (Math.abs(dz)           < 0.92100)              ||
                                       (nMissingHits           <=  3))))
    }
  }
 
  var rhoeffareaUDF = udf {
    val eta_range = Array(0.0f, 0.8f, 1.3f, 2.0f, 2.2f, 2.3f, 2.4f)
    val eff_area = Array(0.1752f, 0.1862f, 0.1411f, 0.1534f, 0.1903f, 0.2243f, 0.2687f)
    (rho: Float, eta: Float) => rho*EffArea(eta, eta_range, eff_area)
  }

  def filterElectronDF(spark: SparkSession, e_df: DataFrame) : DataFrame = {
    import spark.implicits._
    //Old UDF function rhoeffareaUDF
    //var fdf = e_df.withColumn("rhoEffarea", rhoeffareaUDF($"rhoIso", $"Electron_eta"))
    
    //SQL replacement for UDF rhoeffareaUDF
    e_df.createOrReplaceTempView("Electrons")
    var fdf = spark.sql("SELECT *, rhoIso * (CASE WHEN Electron_eta < 0.8 THEN 0.1752 WHEN Electron_eta < 1.3 THEN 0.1862 WHEN Electron_eta < 2.0 THEN 0.1411 WHEN Electron_eta < 2.2 THEN 0.1534 WHEN Electron_eta < 2.3 THEN 0.1903 WHEN Electron_eta < 2.4 THEN 0.2243 WHEN Electron_eta >= 2.4 THEN 0.2687 END) AS rhoEffarea FROM Electrons")    


    //SQL Replacement for the old UDF func for passfilter1
    //fdf.createOrReplaceTempView("elecFilter")
    //fdf = spark.sql("SELECT *, CASE WHEN (Electron_isConv=1) THEN false ELSE (((ABS(Electron_scEta) < 1.479) AND ((iso < (0.126 * Electron_pt)) OR (ABS(Electron_dEtaIn) < 0.01520) OR (ABS(Electron_dPhiIn) < 0.21600) OR (Electron_sieie < 0.01140))) OR ((ABS(Electron_scEta) >= 1.479) AND ((iso < (0.144 * Electron_pt)) OR (ABS(Electron_dEtaIn) < 0.01130) OR (ABS(Electron_dPhiIn) < 0.23700) OR (Electron_sieie < 0.03520)))) END AS passfilter1 FROM (SELECT *, (Electron_chHadIso + GREATEST(CAST(0.0 AS DOUBLE), (Electron_gammaIso + Electron_neuHadIso - rhoEffarea))) AS iso FROM elecFilter)")

    //Old UDF function for passfilter1
    fdf = fdf.withColumn("passfilter1", elecpassUDF($"Electron_pt", $"Electron_isConv", $"Electron_chHadIso", $"Electron_gammaIso", $"Electron_neuHadIso", $"rhoEffarea", $"Electron_scEta", $"Electron_dEtaIn", $"Electron_dPhiIn", $"Electron_sieie"))

    
    //SQL Replacement for passfilter2 UDF func
    fdf.createOrReplaceTempView("elecFilter")
    fdf = spark.sql("SELECT *, (((ABS(Electron_scEta) < 1.479) AND ( (Electron_hovere < 0.18100) OR (ABS(1.0 - Electron_eoverp) < (0.20700* Electron_ecalEnergy)) OR (ABS(Electron_d0) < 0.05640) OR (ABS(Electron_dz) < 0.47200) OR (Electron_nMissingHits <=  2))) OR ((ABS(Electron_scEta) >= 1.479) AND ( (Electron_hovere < 0.11600) OR (ABS(1.0 - Electron_eoverp) < (0.17400* Electron_ecalEnergy)) OR (ABS(Electron_d0) < 0.22200) OR (ABS(Electron_dz) < 0.92100) OR (Electron_nMissingHits <=  3)))) AS passfilter2 FROM elecFilter")

    //Old UDF function for passfiler2
    //fdf = fdf.withColumn("passfilter2", elecpass2UDF($"Electron_scEta", $"Electron_hovere", $"Electron_eoverp", $"Electron_ecalEnergy", $"Electron_d0", $"Electron_dz", $"Electron_nMissingHits"))
    fdf.createOrReplaceTempView("Electrons")
    val fdf1 = spark.sql("SELECT * FROM Electrons WHERE Electron_pt >= 10 and Electron_eta < 2.5 and Electron_eta > -2.5 and passfilter1 and passfilter2")
    fdf1
  }


  /*Filters for Jet and VJet*/
  var jetpassUDF = udf {
    (neuHadFrac: Float, neuEmFrac: Float, nParticles: Int, eta: Float, chHadFrac: Float, nCharged: Int, chEmFrac: Float) => {
       (neuHadFrac >= 0.99) || 
        neuEmFrac >= 0.99   || 
        nParticles <= 1     || 
        ((eta < 2.4 && eta > -2.4) && (chHadFrac == 0 || nCharged == 0 || chEmFrac >= 0.99))
    }
  }

  def filterJetDF(spark: SparkSession, jet_df: DataFrame) : DataFrame = {
    import spark.implicits._
    //OLD UDF filter
    //val fdf = jet_df.withColumn("passfilter", jetpassUDF($"AK4Puppi_neuHadFrac", $"AK4Puppi_neuEmFrac", $"AK4Puppi_nParticles", $"AK4Puppi_eta", $"AK4Puppi_chHadFrac", $"AK4Puppi_nCharged", $"AK4Puppi_chEmFrac"))
    
    //SQL replacement for UDF
    jet_df.createOrReplaceTempView("Jets")
    val fdf = spark.sql("SELECT *, (AK4Puppi_neuHadFrac >= 0.99) OR AK4Puppi_neuEmFrac >= 0.99 OR AK4Puppi_nParticles <= 1 OR ((AK4Puppi_eta < 2.4 AND AK4Puppi_eta > -2.4) AND (AK4Puppi_chHadFrac == 0 OR AK4Puppi_nCharged == 0 OR AK4Puppi_chEmFrac >= 0.99)) AS passfilter FROM Jets")

    fdf.createOrReplaceTempView("Jets")
    val fdf1 = spark.sql("SELECT * FROM Jets WHERE AK4Puppi_pt >=30 and AK4Puppi_eta < 4.5 and AK4Puppi_eta > -4.5 and passfilter")
    fdf1
  }

  def filterVJetDF(spark: SparkSession, vjet_df: DataFrame) : DataFrame = {
    import spark.implicits._
    //Old UDF func
    //val fdf = vjet_df.withColumn("passfilter", jetpassUDF($"CA15Puppi_neuHadFrac", $"CA15Puppi_neuEmFrac", $"CA15Puppi_nParticles", $"CA15Puppi_eta", $"CA15Puppi_chHadFrac", $"CA15Puppi_nCharged", $"CA15Puppi_chEmFrac"))
    
    //SQL Replacement for UDF
    vjet_df.createOrReplaceTempView("Vjets")
    val fdf = spark.sql("SELECT *, (CA15Puppi_neuHadFrac >= 0.99) OR CA15Puppi_neuEmFrac >= 0.99 OR CA15Puppi_nParticles <= 1 OR ((CA15Puppi_eta < 2.4 AND CA15Puppi_eta > -2.4) AND (CA15Puppi_chHadFrac == 0 OR CA15Puppi_nCharged == 0 OR CA15Puppi_chEmFrac >= 0.99)) AS passfilter FROM VJets")


    fdf.createOrReplaceTempView("VJets")
    val fdf1 = spark.sql("SELECT * FROM VJets WHERE CA15Puppi_pt >=150 and CA15Puppi_eta < 2.5 and CA15Puppi_eta > -2.5 and passfilter")
    fdf1
  }
  /* dR filter or deltaR filter*/
  def DeltaRUDF = udf {(eta1: Float, phi1: Float, eta2: Float, phi2: Float) => {
    val diffeta = eta1 - eta2
    val diffphi = phi1 - phi2
    val dphi_adjusted = if (diffphi >= Math.PI) diffphi - Math.PI
                        else if (diffphi < -Math.PI) diffphi + Math.PI
                        else diffphi
    Math.sqrt(diffeta*diffeta + dphi_adjusted*dphi_adjusted)
  }}

}
