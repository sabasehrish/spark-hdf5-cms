import Filters._
import H5Read._
import H5DataFrame._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.sql.functions.{udf, callUDF, lit, col}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.types._

object DF {
  def createMuonDF(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) : DataFrame = {
    import spark.implicits._
    val muon_gn = "/Muon/"
    val muon_ds: List[String] = List("Muon.eta", "Muon.pt", "Muon.phi", "Muon.evtNum", 
      "Muon.runNum", "Muon.lumisec", "Muon.chHadIso", "Muon.neuHadIso", "Muon.puIso", 
      "Muon.pogIDBits", "Muon.gammaIso")
    val partitionlist = getPartitionInfo(dname, muon_gn+"Muon.eta", chunkSize)
    val muon_rdd = sc.parallelize(partitionlist, partitionlist.length).flatMap(x=> readDatasets(x.dname+x.fname, muon_gn, muon_ds, x.begin, x.end))
    val muon_df = createH5DataFrame(spark, muon_rdd, muon_gn)
    muon_df
  }
  
  def createFilteredMuonDF(spark: SparkSession, muon_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val muonMass = 0.105658369
    val fdf = filterMuonDF(spark, 1, muon_df)
    val maxdf = 
      fdf.select($"Muon_runNum", $"Muon_lumisec", $"Muon_evtNum", struct($"Muon_pt", $"Muon_eta", $"Muon_phi").alias("tmpmuons"))
         .groupBy("Muon_runNum", "Muon_lumisec", "Muon_evtNum")
         .agg(max("tmpmuons").alias("tmpmuons"))
         .select($"Muon_runNum", $"Muon_lumisec", $"Muon_evtNum", $"tmpmuons.Muon_pt", $"tmpmuons.Muon_eta", $"tmpmuons.Muon_phi")
    //Old UDF function
    //maxdf.withColumn("Muon_Mass", lit(muonMass))
  
    //SQL Replacement
    maxdf.createOrReplaceTempView("muons")
    spark.sql("SELECT *, " + muonMass + " AS Muon_Mass FROM muons")
  }

  def createTauDF(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) : DataFrame = {
    import spark.implicits._
    val tau_gn = "/Tau/"
    val tau_ds: List[String] = List("Tau.eta", "Tau.pt", "Tau.phi", "Tau.evtNum", "Tau.runNum", "Tau.lumisec", "Tau.hpsDisc", "Tau.rawIso3Hits")
    val tau_pl = getPartitionInfo(dname, tau_gn+"Tau.eta", chunkSize)
    val tau_rdd = sc.parallelize(tau_pl, tau_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, tau_gn, tau_ds, x.begin, x.end))
    val tau_df = createH5DataFrame(spark, tau_rdd, tau_gn)
    tau_df
  }
  def createFilteredTauDF(spark: SparkSession, tau_df: DataFrame) : DataFrame =  {
    import spark.implicits._
    val tau_fdf = filterTauDF(spark, tau_df)
    val ftdf = tau_fdf.select($"Tau_runNum", $"Tau_lumisec", $"Tau_evtNum", struct($"Tau_pt", $"Tau_eta", $"Tau_phi").alias("tmptaus"))
                   .groupBy("Tau_runNum", "Tau_lumisec", "Tau_evtNum")
                   .agg(max("tmpatus").alias("tmptaus"))
                   .select($"Tau_runNum", $"Tau_lumisec", $"Tau_evtNum", $"tmptaus.Tau_pt", $"tmptaus.Tau_eta", $"tmptaus.Tau_phi")
    ftdf
  }

  def createInfoDF(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) : DataFrame = {
    val info_gn = "/Info/"
    val info_ds: List[String] = List("runNum", "lumiSec", "evtNum", "rhoIso", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val info_pl = getPartitionInfo(dname, info_gn+"evtNum", chunkSize)
    val info_rdd = sc.parallelize(info_pl, info_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, info_gn, info_ds, x.begin, x.end))
    val info_df = createH5DataFrame(spark, info_rdd, info_gn)
    info_df.createOrReplaceTempView("infot")
    val fdf = spark.sql("SELECT * FROM infot WHERE pfMET > 200 or puppET > 200")
    fdf
  }

  def createElectronDF(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) : DataFrame = {
    import spark.implicits._
    val elec_gn = "/Electron/"
    val elec_ds: List[String] = List("Electron.runNum", "Electron.lumisec", "Electron.evtNum", "Electron.eta", "Electron.pt", "Electron.phi", "Electron.chHadIso", "Electron.neuHadIso", "Electron.gammaIso", "Electron.scEta", "Electron.sieie", "Electron.hovere", "Electron.eoverp", "Electron.dEtaIn", "Electron.dPhiIn", "Electron.ecalEnergy", "Electron.d0", "Electron.dz", "Electron.nMissingHits", "Electron.isConv")
    val elec_pl = getPartitionInfo(dname, elec_gn+"Electron.eta", chunkSize)
    val elec_rdd = sc.parallelize(elec_pl, elec_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, elec_gn, elec_ds, x.begin, x.end))
    val elec_df = createH5DataFrame(spark, elec_rdd, elec_gn)
    elec_df
  }

  def createFilteredElectronDF(spark: SparkSession, elec_df: DataFrame, info_df: DataFrame) : DataFrame =  {
    import spark.implicits._
    val electronMass = 0.000510998910
    val ftdf = elec_df.join(info_df, 
        elec_df("Electron_runNum") <=> info_df("runNum")
        && elec_df("Electron_lumisec") <=> info_df("lumiSec")
        && elec_df("Electron_evtNum") <=> info_df("evtNum"),
    "left"
)
    val fdf = filterElectronDF(spark, ftdf) 
    val ftdf1 = fdf.select($"Electron_runNum", $"Electron_lumisec", $"Electron_evtNum", struct($"Electron_pt", $"Electron_eta", $"Electron_phi").alias("tmps"))
                  .groupBy("Electron_runNum", "Electron_lumisec", "Electron_evtNum")
                  .agg(max("tmps").alias("tmps"))
                  .select($"Electron_runNum", $"Electron_lumisec", $"Electron_evtNum", $"tmps.Electron_pt", $"tmps.Electron_eta", $"tmps.Electron_phi")
    
    //Old UDF function
    //ftdf1.withColumn("Electron_Mass", lit(electronMass))
  
    //SQL replacement
    ftdf1.createOrReplaceTempView("Electrons")
    spark.sql("SELECT *, " + electronMass + " AS Electron_Mass FROM Electrons")
  }

  def createPhotonDF(sc: SparkContext, spark: SparkSession, dname: String, info_df: DataFrame, chunkSize: Int) : DataFrame = {
     /*Photon DF related operations*/
    import spark.implicits._
    val pho_gn = "/Photon/"
    val pho_ds: List[String] = List("Photon.runNum", "Photon.lumisec", "Photon.evtNum", "Photon.eta", "Photon.pt", "Photon.phi", "Photon.chHadIso", "Photon.scEta", "Photon.neuHadIso", "Photon.gammaIso", "Photon.sieie", "Photon.sthovere")
    val pho_pl = getPartitionInfo(dname, pho_gn+"Photon.eta", chunkSize)
    val pho_rdd = sc.parallelize(pho_pl, pho_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, pho_gn, pho_ds, x.begin, x.end))
    val pho_df = createH5DataFrame(spark, pho_rdd, pho_gn)
    val ftdf = pho_df.join(info_df,
        pho_df("Photon_runNum") <=> info_df("runNum")
        && pho_df("Photon_lumisec") <=> info_df("lumiSec")
        && pho_df("Photon_evtNum") <=> info_df("evtNum"),
    "left"
)
  val maxdf = ftdf.select($"Photon_runNum", $"Photon_lumisec", $"Photon_evtNum", struct($"Photon_pt", $"Photon_eta", $"Photon_phi").alias("vs"))
                    .groupBy("Photon_runNum", "Photon_lumisec", "Photon_evtNum")
                    .agg(max("vs").alias("vs"), count(lit(1)).alias("NMedium"))
                    .select($"Photon_runNum", $"Photon_lumisec", $"Photon_evtNum", $"vs.Photon_pt", $"vs.Photon_eta", $"vs.Photon_phi", $"NMedium")
    maxdf
  }

  def createGenInfoDF(sc: SparkContext, spark: SparkSession, dname: String, chunkSize: Int) : DataFrame = {
       /*Gen Event Info to calculate sum of weights*/
    val genevtinfo_gn = "/GenEvtInfo/"
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val genevtinfo_pl = getPartitionInfo(dname, genevtinfo_gn+"weight", chunkSize)
    val genevtinfo_rdd = sc.parallelize(genevtinfo_pl, genevtinfo_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, genevtinfo_gn, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createH5DataFrame(spark, genevtinfo_rdd, genevtinfo_gn)
    genevtinfo_df
  }

  def createJetDF(sc: SparkContext, spark: SparkSession, dname: String, info_df: DataFrame, vjet_df: DataFrame, chunkSize: Int) : DataFrame = {
    import spark.implicits._
    val jet_gn = "/AK4Puppi/"
    val jet_ds: List[String] = List("AK4Puppi.runNum", "AK4Puppi.lumisec", "AK4Puppi.evtNum", "AK4Puppi.eta", "AK4Puppi.pt", "AK4Puppi.phi", "AK4Puppi.chHadFrac", "AK4Puppi.chEmFrac", "AK4Puppi.neuHadFrac", "AK4Puppi.neuEmFrac", "AK4Puppi.mass", "AK4Puppi.csv", "AK4Puppi.nParticles", "AK4Puppi.nCharged" )
    val jet_pl = getPartitionInfo(dname, jet_gn+"AK4Puppi.eta", chunkSize)
    val jet_rdd = sc.parallelize(jet_pl, jet_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, jet_gn, jet_ds, x.begin, x.end))
    val jet_df = createH5DataFrame(spark, jet_rdd, jet_gn)
    //Step 1: Join with Info DF
    val jet_df1 = jet_df.join(info_df,
        jet_df("AK4Puppi_runNum") <=> info_df("runNum")
        && jet_df("AK4Puppi_lumisec") <=> info_df("lumiSec")
        && jet_df("AK4Puppi_evtNum") <=> info_df("evtNum"),
    "left"
) 
  //Step 2: filter
    val fJetDF = filterJetDF(spark, jet_df1).drop("passfilter", "AK4Puppi_nParticles", "AK4Puppi_nCharged", "AK4Puppi_chEMFrac")
    //Step 3: Max, count and self join
    var tdf1 = fJetDF.select($"AK4Puppi_runNum", $"AK4Puppi_lumisec", $"AK4Puppi_evtNum", struct($"AK4Puppi_pt", $"AK4Puppi_eta", $"AK4Puppi_phi", $"AK4Puppi_csv", $"puppETphi").alias("vs"))
                    .groupBy("AK4Puppi_runNum", "AK4Puppi_lumisec", "AK4Puppi_evtNum")
                    .agg(max("vs").alias("vs"), count(lit(1)).alias("N"))
                    .select($"AK4Puppi_runNum", $"AK4Puppi_lumisec", $"AK4Puppi_evtNum", $"vs.AK4Puppi_pt", $"vs.AK4Puppi_eta", $"vs.AK4Puppi_phi", $"vs.AK4Puppi_csv", $"vs.puppETphi", $"N")
    //Step 4: Add new columns
    //Old UDF functions
    //tdf1 = tdf1.withColumn("mindPhi", pdPhiUDF(999.99f)($"puppETphi", $"AK4Puppi_phi"))
    //tdf1 = tdf1.withColumn("mindFPhi", pdFPhiUDF(999.99f)($"puppETphi", $"AK4Puppi_phi"))
    
    //SWL replacements
    tdf1.createOrReplaceTempView("phi")
    tdf1 = spark.sql("SELECT *, CASE WHEN ACOS(COS( puppETphi - AK4Puppi_phi )) < 999.99 THEN ACOS(COS(puppEtphi - AK4puppi_phi)) ELSE 999.99 END AS mindPhi FROM phi")

    tdf1.createOrReplaceTempView("phi")
    tdf1 = spark.sql("SELECT *, CASE WHEN (puppETphi > 0) AND (ACOS(COS(puppETphi - AK4Puppi_phi)) < 999.99) THEN ACOS(COS(puppETphi - AK4Puppi_phi)) ELSE 999.99 END AS mindFPhi FROM phi")

    // Step 5: Join with Vjets
    var tdf2 = tdf1.join(vjet_df,
        tdf1("AK4Puppi_runNum") <=> vjet_df("CA15Puppi_runNum")
        && tdf1("AK4Puppi_lumisec") <=> vjet_df("CA15Puppi_lumisec")
        && tdf1("AK4Puppi_evtNum") <=> vjet_df("CA15Puppi_evtNum"),
    "left"
)
    //Step 6: more filters 
    //OLD UDF Function to create deltaR col
    //tdf2 = tdf2.withColumn("deltaR", DeltaRUDF($"AK4Puppi_eta", $"AK4Puppi_phi", $"CA15Puppi_eta", $"CA15Puppi_phi"))
    
    //SQL replacement for UDF
    tdf2.createOrReplaceTempView("tdf2")
    tdf2 = spark.sql("SELECT *, SQRT(((AK4Puppi_eta - CA15Puppi_eta)*(AK4Puppi_eta - CA15Puppi_eta)) + (dphi_adjusted * dphi_adjusted)) AS deltaR FROM (SELECT *, CASE WHEN (AK4Puppi_phi - CA15Puppi_phi) >= PI() THEN (AK4Puppi_phi - CA15Puppi_phi) - PI() WHEN (AK4Puppi_phi - CA15Puppi_phi) < -PI() THEN (AK4Puppi_phi - CA15Puppi_phi) + PI() ELSE (AK4Puppi_phi - CA15Puppi_phi) END AS dphi_adjusted FROM tdf2)")


    tdf2.createOrReplaceTempView("tdf2")
    val tdf3 = spark.sql("SELECT * from tdf2 WHERE deltaR > 1.5").groupBy("AK4Puppi_runNum", "AK4Puppi_lumiSec", "AK4Puppi_evtNum").count()
    val tdf4 = spark.sql("SELECT * from tdf2 WHERE deltaR > 1.5 and AK4Puppi_eta < 2.5 and AK4Puppi_eta > -2.5 and AK4Puppi_csv > 0.605").groupBy("AK4Puppi_runNum", "AK4Puppi_lumiSec", "AK4Puppi_evtNum").count()
    tdf3.createOrReplaceTempView("count1")
    tdf4.createOrReplaceTempView("count2")
    tdf2.createOrReplaceTempView("filteredJets")
    val countmax = spark.sql("SELECT count1.AK4Puppi_runNum as tmp_runNum, count1.AK4Puppi_lumisec as tmp_lumiSec, count1.AK4Puppi_evtNum as tmp_evtNum, count1.count as NdR15, count2.count as NbtagLdR15 from count1, count2 WHERE count1.AK4Puppi_runNum == count2.AK4Puppi_runNum and count1.AK4Puppi_lumisec == count2.AK4Puppi_lumisec and count1.AK4Puppi_evtNum == count2.AK4Puppi_evtNum")
    countmax.createOrReplaceTempView("countmax")
    val tdf = spark.sql("SELECT * FROM filteredJets, countmax WHERE filteredJets.AK4Puppi_runNum == countmax.tmp_runNum and filteredJets.AK4Puppi_lumiSec == countmax.tmp_lumiSec and filteredJets.AK4Puppi_evtNum == countmax.tmp_evtNum").drop("tmp_lumiSec", "tmp_runNum", "tmp_evtNum")
    tdf
  }


//UDF to calculate the pdPhi
  def pdPhiUDF(pdPhi: Float) = udf ((puppETphi:Float, phi: Float) => {
                                 if(Math.acos(Math.cos(puppETphi-phi)) < pdPhi)
                                   Math.acos(Math.cos(puppETphi-phi))
                                 else pdPhi })

//UDF to calculate the pdfPhi
  def pdFPhiUDF(pdFPhi: Float) = udf ((puppETphi:Float, phi: Float) => {
                                 if(puppETphi > 0 && Math.acos(Math.cos(puppETphi-phi)) < pdFPhi)
                                   Math.acos(Math.cos(puppETphi-phi))
                                 else pdFPhi })

//add Index to a DataFrame
  def addIndex(df: DataFrame, spark: SparkSession) : DataFrame = {
    spark.createDataFrame(
    // Add index
    df.rdd.zipWithIndex.map{case (r, i) => Row.fromSeq(r.toSeq :+ i)},
    // Create schema
    StructType(df.schema.fields :+ StructField("_index", LongType, false))
    )
  }

  def createVJetDF(sc: SparkContext, spark: SparkSession, dname: String, vaddjet_df: DataFrame, chunkSize: Int) : DataFrame = {
    val vjet_gn = "/CA15Puppi/"
    val vjet_ds: List[String] = List("CA15Puppi.runNum", "CA15Puppi.lumisec", "CA15Puppi.evtNum", "CA15Puppi.eta", "CA15Puppi.pt", "CA15Puppi.phi", "CA15Puppi.chHadFrac", "CA15Puppi.chEmFrac", "CA15Puppi.neuHadFrac", "CA15Puppi.neuEmFrac", "CA15Puppi.mass", "CA15Puppi.csv", "CA15Puppi.nParticles", "CA15Puppi.nCharged" )
    val vjet_pl = getPartitionInfo(dname, vjet_gn+"CA15Puppi.eta", chunkSize)
    val vjet_rdd = sc.parallelize(vjet_pl, vjet_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, vjet_gn, vjet_ds, x.begin, x.end))
    val vjet_df = createH5DataFrame(spark, vjet_rdd, vjet_gn)
    import spark.implicits._
    val ai = addIndex(vjet_df, spark)
    val bi = addIndex(vaddjet_df, spark)
    ai.createOrReplaceTempView("Vjet")
    bi.createOrReplaceTempView("Addjet")
    /*var tdf = ai
             .join(bi, Seq("_index"))
             .drop("_index", "AddCA15Puppi_runNum", "AddCA15Puppi_lumisec", "AddCA15Puppi_evtNum")
    */
    var tdf = spark.sql("SELECT * FROM Vjet, Addjet WHERE Vjet._index == Addjet._index").drop("AddCA15Puppi_runNum", "AddCA15Puppi_lumisec", "AddCA15Puppi_evtNum", "_index")

    val fvjet_df = filterVJetDF(spark, tdf)
    val maxdf = fvjet_df.select($"CA15Puppi_runNum", $"CA15Puppi_lumisec", $"CA15Puppi_evtNum", struct($"CA15Puppi_pt", $"CA15Puppi_eta", 
                                $"CA15Puppi_phi", $"CA15Puppi_mass", $"CA15Puppi_csv", $"CA15Puppi_chHadFrac", $"CA15Puppi_neuHadFrac", 
                                $"CA15Puppi_neuEMFrac", $"tau21", $"tau32", $"mincsv", $"maxsubcsv", $"AddCA15Puppi_mass_sd0").alias("vs"))
                        .groupBy("CA15Puppi_runNum", "CA15Puppi_lumisec", "CA15Puppi_evtNum")
                        .agg(max("vs").alias("vs"), count(lit(1)).alias("N"))
                        .select($"CA15Puppi_runNum", $"CA15Puppi_lumisec", $"CA15Puppi_evtNum", $"vs.CA15Puppi_pt", $"vs.CA15Puppi_eta", 
                                $"vs.CA15Puppi_phi", $"N", $"vs.CA15Puppi_mass", $"vs.CA15Puppi_csv", $"vs.CA15Puppi_chHadFrac", 
                                $"vs.CA15Puppi_neuHadFrac", $"vs.CA15Puppi_neuEMFrac", $"vs.tau21", $"vs.tau32", $"vs.mincsv", 
                                $"vs.maxsubcsv", $"vs.AddCA15Puppi_mass_sd0")
    maxdf
  }

  def createVAddJetDF(sc: SparkContext, spark: SparkSession, dname:String, chunkSize: Int) : DataFrame = {
    import spark.implicits._
    val vaddjet_ds: List[String]= List("AddCA15Puppi.runNum", "AddCA15Puppi.lumisec", "AddCA15Puppi.evtNum", "AddCA15Puppi.tau1", "AddCA15Puppi.tau2", "AddCA15Puppi.tau3", "AddCA15Puppi.mass_sd0", "AddCA15Puppi.sj1_csv", "AddCA15Puppi.sj2_csv", "AddCA15Puppi.sj3_csv",  "AddCA15Puppi.sj4_csv")
    val vaddjet_gn = "/AddCA15Puppi/"
    val vjet_pl = getPartitionInfo(dname, vaddjet_gn+"AddCA15Puppi.tau1", chunkSize)
    val vjet_rdd = sc.parallelize(vjet_pl, vjet_pl.length).flatMap(x=> readDatasets(x.dname+x.fname, vaddjet_gn, vaddjet_ds, x.begin, x.end))
    var vjet_df = createH5DataFrame(spark, vjet_rdd, vaddjet_gn)
    
    //Old UDF functions
    //vjet_df = vjet_df.withColumn("tau21", taudivUDF($"AddCA15Puppi_tau1", $"AddCA15Puppi_tau2"))
    //vjet_df = vjet_df.withColumn("tau32", taudivUDF($"AddCA15Puppi_tau2", $"AddCA15Puppi_tau3"))
    //vjet_df = vjet_df.withColumn("mincsv", mincsvUDF($"AddCA15Puppi_sj1_csv", $"AddCA15Puppi_sj2_csv"))
    //vjet_df = vjet_df.withColumn("maxsubcsv", maxsubcsvUDF($"AddCA15Puppi_sj1_csv", $"AddCA15Puppi_sj2_csv", $"AddCA15Puppi_sj3_csv", $"AddCA15Puppi_sj4_csv"))
    
    //SQL Replacement
    //taudivUDF
    vjet_df.createOrReplaceTempView("vjet")
    vjet_df = spark.sql("SELECT *, AddCA15Puppi_tau1/AddCA15Puppi_tau2 AS tau21, AddCA15Puppi_tau2/AddCA15Puppi_tau3 AS tau32, LEAST(AddCA15Puppi_sj1_csv, AddCA15Puppi_sj2_csv) AS mincsv, GREATEST(AddCA15Puppi_sj1_csv, AddCA15Puppi_sj2_csv, AddCA15Puppi_sj3_csv, AddCA15Puppi_sj4_csv) AS maxsubcsv FROM vjet")
    

    vjet_df.drop("AddCA15Puppi_tau1", "AddCA15Puppi_tau2", "AddCA15Puppi_tau3", "AddCA15Puppi_sj1_csv", "AddCA15Puppi_sj2_csv", "AddCA15Puppi_sj3_csv", "AddCA15Puppi_sj4_csv")
  }

  def taudivUDF= udf ((tauA:Float, tauB: Float) => tauB/tauA)
  def mincsvUDF = udf ((sj1_csv: Float, sj2_csv: Float) => Math.min(sj1_csv, sj2_csv))
  def maxsubcsvUDF = udf ((sj1_csv: Float, sj2_csv: Float, sj3_csv: Float, sj4_csv: Float) => Math.max(Math.max(sj1_csv, sj2_csv),Math.max(sj3_csv, sj4_csv)))

}

