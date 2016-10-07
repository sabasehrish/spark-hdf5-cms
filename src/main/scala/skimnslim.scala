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

object SkimWF {
  def skimworkflow(sc: SparkContext, spark: SparkSession, dname: String) {
    import spark.implicits._
    // read all HDF5 groups and data sets that are needed for this analysis 
    // into Spark DataFrames
    // Apply filters on each DF and get the resulting DF 

    var t0 = System.nanoTime()
    val info_df = createInfoDF(sc, spark, dname)
    info_df.cache()
    info_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/info2.csv")
    var t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Info DF")   

    t0 = System.nanoTime()
    val addjet_df = createVAddJetDF(sc, spark, dname)
    val vjet_df = createVJetDF(sc, spark, dname, addjet_df)
    vjet_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/vjet2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save VJet DF")
    
    t0 = System.nanoTime()
    val jet_df = createJetDF(sc, spark, dname, info_df, vjet_df)
    vjet_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/jet2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Jet DF")
  
    t0 = System.nanoTime()
    val muon_df = createMuonDF(sc, spark, dname)
    muon_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/muons2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Muon DF") 

    t0 = System.nanoTime()
    val elec_df = createElectronDF(sc, spark, dname, info_df)
    elec_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/electrons2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Electron DF")

    t0 = System.nanoTime()
    val pho_df = createPhotonDF(sc, spark, dname, info_df)
    pho_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/pho2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Photon DF")
    
    t0 = System.nanoTime()
    val tau_df = createTauDF(sc, spark, dname)
    tau_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/tau2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Tau DF")

    t0 = System.nanoTime()
    val genevtinfo_df = createGenInfoDF(sc, spark, dname)
    genevtinfo_df.write.format("com.databricks.spark.csv").option("header","true").save("/global/cscratch1/sd/ssehrish/geninfo2.csv")
    t1 = System.nanoTime()
    println("It took :" + (t1 - t0) +" ns to create and save Gen Info DF")
  }

  def createMuonDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
    import spark.implicits._
    val muonMass = 0.105658369
    val muon_gn = "/Muon/"
    val muon_ds: List[String] = List("Muon.eta", "Muon.pt", "Muon.phi", "Muon.evtNum", "Muon.runNum", "Muon.lumisec", "Muon.chHadIso", "Muon.neuHadIso", "Muon.puIso", "Muon.pogIDBits", "Muon.gammaIso")
    val partitionlist = getPartitionInfo(dname, muon_gn+"Muon.eta")
    val muon_rdd = sc.parallelize(partitionlist, partitionlist.length).flatMap(x=> readDatasets(dname+x.fname, muon_gn, muon_ds, x.begin, x.end))
    val muon_df = createDataFrame(spark, muon_rdd, muon_gn, muon_ds)
    val fdf = filterMuonDF(spark, 1, muon_df)
    fdf.cache()
    val newNames = Seq("lumiSec", "runNum", "evtNum", "pt")
    val mdf = fdf.groupBy("Muon_lumisec", "Muon_runNum", "Muon_evtNum").max("Muon_pt").toDF(newNames: _*)
    fdf.createOrReplaceTempView("filteredMuons")
    mdf.createOrReplaceTempView("mdf")
    val mdf1 = spark.sql("SELECT * FROM filteredMuons, mdf WHERE filteredMuons.Muon_runNum == mdf.runNum and filteredMuons.Muon_lumiSec == mdf.lumiSec and filteredMuons.Muon_evtNum == mdf.evtNum and filteredMuons.Muon_pt == mdf.pt").drop("pt", "runNum", "lumiSec", "evtNum", "Muon_chHadIso", "Muon_neuHadIso", "Muon_puIso", "Muon_pogIDBits", "Muon_gammaIso")
    mdf1.withColumn("Muon_Mass", lit(muonMass))
  }

  def createTauDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
    import spark.implicits._
    val tau_gn = "/Tau/"
    val tau_ds: List[String] = List("Tau.eta", "Tau.pt", "Tau.phi", "Tau.evtNum", "Tau.runNum", "Tau.lumisec", "Tau.hpsDisc", "Tau.rawIso3Hits")
    val tau_pl = getPartitionInfo(dname, tau_gn+"Tau.eta")
    val tau_rdd = sc.parallelize(tau_pl, tau_pl.length).flatMap(x=> readDatasets(dname+x.fname, tau_gn, tau_ds, x.begin, x.end))
    val tau_df = createDataFrame(spark, tau_rdd, tau_gn, tau_ds)
    val tau_fdf = filterTauDF(spark, tau_df)
    tau_fdf.cache()
    val newNames = Seq("lumiSec", "runNum", "evtNum", "pt")
    val tdf = tau_fdf.groupBy("Tau_lumisec", "Tau_runNum", "Tau_evtNum").max("Tau_pt").toDF(newNames: _*)
    tau_df.createOrReplaceTempView("TauDF")
    tdf.createOrReplaceTempView("Tdf")
    val ftdf = spark.sql("SELECT * FROM TauDF, Tdf WHERE TauDF.Tau_lumisec == Tdf.lumiSec and TauDF.Tau_runNum == Tdf.runNum and TauDF.Tau_evtNum == Tdf.evtNum and TauDF.Tau_pt == Tdf.pt") .drop("pt", "runNum", "lumiSec", "evtNum", "Tau_hpsDisc", "Tau_rawIso3Hits")
    ftdf
  }

  def createInfoDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
    val info_gn = "/Info/"
    val info_ds: List[String] = List("runNum", "lumiSec", "evtNum", "rhoIso", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val info_pl = getPartitionInfo(dname, info_gn+"evtNum")
    val info_rdd = sc.parallelize(info_pl, info_pl.length).flatMap(x=> readDatasets(dname+x.fname, info_gn, info_ds, x.begin, x.end))
    val info_df = createDataFrame(spark, info_rdd, info_gn, info_ds)
    info_df.createOrReplaceTempView("infot")
    val fdf = spark.sql("SELECT * FROM infot WHERE pfMET > 200 or puppET > 200")
    fdf
  }

  def createElectronDF(sc: SparkContext, spark: SparkSession, dname: String, info_df: DataFrame) : DataFrame = {
    val electronMass = 0.000510998910
    val elec_gn = "/Electron/"
    val elec_ds: List[String] = List("Electron.runNum", "Electron.lumisec", "Electron.evtNum", "Electron.eta", "Electron.pt", "Electron.phi", "Electron.chHadIso", "Electron.neuHadIso", "Electron.gammaIso", "Electron.scEta", "Electron.sieie", "Electron.hovere", "Electron.eoverp", "Electron.dEtaIn", "Electron.dPhiIn", "Electron.ecalEnergy", "Electron.d0", "Electron.dz", "Electron.nMissingHits", "Electron.isConv")
    val elec_pl = getPartitionInfo(dname, elec_gn+"Electron.eta")
    val elec_rdd = sc.parallelize(elec_pl, elec_pl.length).flatMap(x=> readDatasets(dname+x.fname, elec_gn, elec_ds, x.begin, x.end))
    val elec_df = createDataFrame(spark, elec_rdd, elec_gn, elec_ds)
    info_df.createOrReplaceTempView("InfoDF")
    elec_df.createOrReplaceTempView("Tdf")
    val ftdf = spark.sql("SELECT * FROM InfoDF, Tdf WHERE InfoDF.evtNum == Tdf.Electron_evtNum and InfoDF.lumiSec == Tdf.Electron_lumisec and infoDF.runNum == Tdf.Electron_runNum").drop("runNum", "evtNum", "lumiSec", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val fdf = filterElectronDF(spark, ftdf).drop("rhoEffarea", "passfilter1", "passfilter2", "rhoIso")
    fdf.cache()
    val newNames = Seq("runNum", "lumiSec", "evtNum" ,"pt")
    val mdf = fdf.groupBy("ELectron_runNum", "Electron_lumisec", "Electron_evtNum").max("Electron_pt").toDF(newNames: _*)
    fdf.createOrReplaceTempView("filteredElectrons")
    mdf.createOrReplaceTempView("mdf")
    val mdf1 = spark.sql("SELECT * FROM filteredElectrons, mdf WHERE filteredElectrons.Electron_runNum == mdf.runNum and filteredElectrons.Electron_lumiSec == mdf.lumiSec and filteredElectrons.Electron_pt == mdf.pt").drop("pt", "runNum", "lumiSec", "evtNum", "Electron_chHadIso", "Electron_neuHadIso", "Electron_gammaIso", "Electron_scEta", "Electron_sieie", "Electron_hovere", "Electron_eoverp", "Electron_dEtaIn", "Electron_dPhiIn", "Electron_ecalEnergy", "Electron_d0", "Electron_dz", "Electron_nMissingHits", "Electron_isConv")
    mdf1.withColumn("Electron_Mass", lit(electronMass))
  }

  def createPhotonDF(sc: SparkContext, spark: SparkSession, dname: String, info_df: DataFrame) : DataFrame = {
     /*Photon DF related operations*/
    val pho_gn = "/Photon/"
    val pho_ds: List[String] = List("Photon.runNum", "Photon.lumisec", "Photon.evtNum", "Photon.eta", "Photon.pt", "Photon.phi", "Photon.chHadIso", "Photon.scEta", "Photon.neuHadIso", "Photon.gammaIso", "Photon.sieie", "Photon.sthovere")
    val pho_pl = getPartitionInfo(dname, pho_gn+"Photon.eta")
    val pho_rdd = sc.parallelize(pho_pl, pho_pl.length).flatMap(x=> readDatasets(dname+x.fname, pho_gn, pho_ds, x.begin, x.end))
    val pho_df = createDataFrame(spark, pho_rdd, pho_gn, pho_ds)
    info_df.createOrReplaceTempView("InfoDF")
    pho_df.createOrReplaceTempView("Tdf")
    val ftdf = spark.sql("SELECT * FROM InfoDF, Tdf WHERE InfoDF.evtNum == Tdf.Photon_evtNum and InfoDF.lumiSec == Tdf.Photon_lumisec and infoDF.runNum == Tdf.Photon_runNum").drop("Photon_runNum", "Photon_evtNum", "Photon_lumisec", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val max_tdf = filterPhotonDF(spark, ftdf).groupBy("runNum", "lumiSec", "evtNum").max("Photon_pt").withColumnRenamed("max(Photon_pt)", "Photon_pt")
    val count_tdf = filterPhotonDF(spark, ftdf).groupBy("runNum", "lumiSec", "evtNum").count()
    max_tdf.createOrReplaceTempView("maxpt")
    count_tdf.createOrReplaceTempView("count")
    val tdf = spark.sql("SELECT maxpt.runNum as tmp_runNum, maxpt.lumiSec as tmp_lumiSec, maxpt.evtNum as tmp_evtNum, maxpt.Photon_pt as pt, count.count from maxpt, count WHERE maxpt.runNum == count.runNum and maxpt.lumiSec == count.lumiSec and maxpt.evtNum == count.evtNum")
    ftdf.createOrReplaceTempView("filteredPhotons")
    tdf.createOrReplaceTempView("countmax")
    val tdf1 = spark.sql("SELECT * FROM filteredPhotons, countmax WHERE filteredPhotons.runNum == countmax.tmp_runNum and filteredPhotons.lumiSec == countmax.tmp_lumiSec and filteredPhotons.Photon_pt == countmax.pt").drop("tmp_lumiSec", "tmp_runNum", "pt", "tmp_evtNum", "Photon_chHadIso", "Photon_scEta", "Photon_neuHadIso", "Photon_gammaIso", "Photon_sieie", "Photon_sthovere")
    tdf1
  }

  def createGenInfoDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
       /*Gen Event Info to calculate sum of weights*/
    val genevtinfo_gn = "/GenEvtInfo/"
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val genevtinfo_pl = getPartitionInfo(dname, genevtinfo_gn+"weight")
    val genevtinfo_rdd = sc.parallelize(genevtinfo_pl, genevtinfo_pl.length).flatMap(x=> readDatasets(dname+x.fname, genevtinfo_gn, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createDataFrame(spark, genevtinfo_rdd, genevtinfo_gn, genevtinfo_ds)
    genevtinfo_df
  }

  def createJetDF(sc: SparkContext, spark: SparkSession, dname: String, info_df: DataFrame, vjet_df: DataFrame) : DataFrame = {
    import spark.implicits._
    val jet_gn = "/AK4Puppi/"
    val jet_ds: List[String] = List("AK4Puppi.runNum", "AK4Puppi.lumisec", "AK4Puppi.evtNum", "AK4Puppi.eta", "AK4Puppi.pt", "AK4Puppi.phi", "AK4Puppi.chHadFrac", "AK4Puppi.chEmFrac", "AK4Puppi.neuHadFrac", "AK4Puppi.neuEmFrac", "AK4Puppi.mass", "AK4Puppi.csv", "AK4Puppi.nParticles", "AK4Puppi.nCharged" )
    val jet_pl = getPartitionInfo(dname, jet_gn+"AK4Puppi.eta")
    val jet_rdd = sc.parallelize(jet_pl, jet_pl.length).flatMap(x=> readDatasets(dname+x.fname, jet_gn, jet_ds, x.begin, x.end))
    val jet_df = createDataFrame(spark, jet_rdd, jet_gn, jet_ds)
    info_df.createOrReplaceTempView("InfoDF")
    jet_df.createOrReplaceTempView("Tdf")
    val jet_df1 = spark.sql("SELECT * FROM InfoDF, Tdf WHERE InfoDF.evtNum == Tdf.AK4Puppi_evtNum and InfoDF.lumiSec == Tdf.AK4Puppi_lumisec and infoDF.runNum == Tdf.AK4Puppi_runNum").drop("runNum", "evtNum", "lumiSec", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "rhoIso")
    val fJetDF = filterJetDF(spark, jet_df1).drop("passfilter", "AK4Puppi_nParticles", "AK4Puppi_nCharged", "AK4Puppi_chEMFrac")
    val max_tdf = fJetDF.groupBy("AK4Puppi_runNum", "AK4Puppi_lumiSec", "AK4Puppi_evtNum").max("AK4Puppi_pt").withColumnRenamed("max(AK4Puppi_pt)", "pt")
    val count_tdf = fJetDF.groupBy("AK4Puppi_runNum", "AK4Puppi_lumiSec", "AK4Puppi_evtNum").count()
    max_tdf.createOrReplaceTempView("maxpt")
    count_tdf.createOrReplaceTempView("count")
    var tdf = spark.sql("SELECT maxpt.AK4Puppi_runNum as tmp_runNum, maxpt.AK4Puppi_lumiSec as tmp_lumiSec, maxpt.AK4Puppi_evtNum as tmp_evtNum, maxpt.pt, count.count as N from maxpt, count WHERE maxpt.AK4Puppi_runNum == count.AK4Puppi_runNum and maxpt.AK4Puppi_lumiSec == count.AK4Puppi_lumiSec and maxpt.AK4Puppi_evtNum == count.AK4Puppi_evtNum")
    fJetDF.createOrReplaceTempView("filteredJets")
    tdf.createOrReplaceTempView("countmax")
    var tdf1 = spark.sql("SELECT * FROM filteredJets, countmax WHERE filteredJets.AK4Puppi_runNum == countmax.tmp_runNum and filteredJets.AK4Puppi_lumiSec == countmax.tmp_lumiSec and filteredJets.AK4Puppi_pt == countmax.pt and filteredJets.AK4Puppi_evtNum == countmax.tmp_evtNum").drop("tmp_lumiSec", "tmp_runNum", "pt")
    tdf1 = tdf1.withColumn("mindPhi", pdPhiUDF(999.99f)($"puppETphi", $"AK4Puppi_phi"))
    tdf1 = tdf1.withColumn("mindFPhi", pdFPhiUDF(999.99f)($"puppETphi", $"AK4Puppi_phi"))
    tdf1.createOrReplaceTempView("filteredJ")
    vjet_df.createOrReplaceTempView("filteredVJets")
    var tdf2 = spark.sql("SELECT * from filteredJ, filteredVJets WHERE filteredJ.AK4Puppi_runNum == filteredVJets.CA15Puppi_runNum and filteredJ.AK4Puppi_lumiSec == filteredVJets.CA15Puppi_lumiSec and filteredJ.AK4Puppi_evtNum == filteredVJets.CA15Puppi_evtNum").drop("CA15Puppi_runNum", "CA15Puppi_lumisec" ,"CA15Puppi_evtNum", "CA15Puppi_pt", "CA15Puppi_chHadFrac", "CA15Puppi_neuHadFrac", "CA15Puppi_neuEmFrac", "CA15Puppi_mass", "CA15Puppi_csv", "AddCA15Puppi_mass_sd0", "N", "puppETphi", "tau21", "tau32", "mincsv", "maxsubcsv")
    tdf2 = tdf2.withColumn("deltaR", DeltaRUDF($"AK4Puppi_eta", $"AK4Puppi_phi", $"CA15Puppi_eta", $"CA15Puppi_phi"))
    tdf2.createOrReplaceTempView("tdf2")
    val tdf3 = spark.sql("SELECT * from tdf2 WHERE deltaR > 1.5").groupBy("AK4Puppi_runNum", "AK4Puppi_lumiSec", "AK4Puppi_evtNum").count()
    val tdf4 = spark.sql("SELECT * from tdf2 WHERE deltaR > 1.5 and AK4Puppi_eta < 2.5 and AK4Puppi_eta > -2.5 and AK4Puppi_csv > 0.605").groupBy("AK4Puppi_runNum", "AK4Puppi_lumiSec", "AK4Puppi_evtNum").count()
    tdf3.createOrReplaceTempView("count1")
    tdf4.createOrReplaceTempView("count2")
    tdf2.createOrReplaceTempView("filteredJets")
    val countmax = spark.sql("SELECT count1.AK4Puppi_runNum as tmp_runNum, count1.AK4Puppi_lumisec as tmp_lumiSec, count1.AK4Puppi_evtNum as tmp_evtNum, count1.count as NdR15, count2.count as NbtagLdR15 from count1, count2 WHERE count1.AK4Puppi_runNum == count2.AK4Puppi_runNum and count1.AK4Puppi_lumisec == count2.AK4Puppi_lumisec and count1.AK4Puppi_evtNum == count2.AK4Puppi_evtNum")
    countmax.createOrReplaceTempView("countmax")
    tdf = spark.sql("SELECT * FROM filteredJets, countmax WHERE filteredJets.AK4Puppi_runNum == countmax.tmp_runNum and filteredJets.AK4Puppi_lumiSec == countmax.tmp_lumiSec and filteredJets.AK4Puppi_evtNum == countmax.tmp_evtNum").drop("tmp_lumiSec", "tmp_runNum", "tmp_evtNum")
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

  def createVJetDF(sc: SparkContext, spark: SparkSession, dname: String, vaddjet_df: DataFrame) : DataFrame = {
    val vjet_gn = "/CA15Puppi/"
    val vjet_ds: List[String] = List("CA15Puppi.runNum", "CA15Puppi.lumisec", "CA15Puppi.evtNum", "CA15Puppi.eta", "CA15Puppi.pt", "CA15Puppi.phi", "CA15Puppi.chHadFrac", "CA15Puppi.chEmFrac", "CA15Puppi.neuHadFrac", "CA15Puppi.neuEmFrac", "CA15Puppi.mass", "CA15Puppi.csv", "CA15Puppi.nParticles", "CA15Puppi.nCharged" )
    val vjet_pl = getPartitionInfo(dname, vjet_gn+"CA15Puppi.eta")
    val vjet_rdd = sc.parallelize(vjet_pl, vjet_pl.length).flatMap(x=> readDatasets(dname+x.fname, vjet_gn, vjet_ds, x.begin, x.end))
    val vjet_df = createDataFrame(spark, vjet_rdd, vjet_gn, vjet_ds)
    vaddjet_df.createOrReplaceTempView("Addjet")
    vjet_df.createOrReplaceTempView("Vjet")
    var tdf = spark.sql("SELECT * FROM Vjet, Addjet WHERE Vjet.CA15Puppi_runNum == Addjet.AddCA15Puppi_runNum and Vjet.CA15Puppi_lumisec == Addjet.AddCA15Puppi_lumisec and Vjet.CA15Puppi_evtNum == Addjet.AddCA15Puppi_evtNum").drop("AddCA15Puppi_runNum", "AddCA15Puppi_lumisec", "AddCA15Puppi_evtNum")
    val fvjet_df = filterVJetDF(spark, tdf).drop("passfilter", "CA15Puppi_nParticles", "CA15Puppi_nCharged", "CA15Puppi_chEmFrac")
    val max_tdf = fvjet_df.groupBy("CA15Puppi_runNum", "CA15Puppi_lumisec", "CA15Puppi_evtNum").max("CA15Puppi_pt").withColumnRenamed("max(CA15Puppi_pt)", "pt")
    val count_tdf = fvjet_df.groupBy("CA15Puppi_runNum", "CA15Puppi_lumisec", "CA15Puppi_evtNum").count()
    count_tdf.printSchema
    max_tdf.createOrReplaceTempView("maxpt")
    count_tdf.createOrReplaceTempView("count")
    fvjet_df.createOrReplaceTempView("filteredJets")
    val countmax = spark.sql("SELECT maxpt.CA15Puppi_runNum as tmp_runNum, maxpt.CA15Puppi_lumisec as tmp_lumiSec, maxpt.CA15Puppi_evtNum as tmp_evtNum, maxpt.pt, count.count as N from maxpt, count WHERE maxpt.CA15Puppi_runNum == count.CA15Puppi_runNum and maxpt.CA15Puppi_lumisec == count.CA15Puppi_lumisec and maxpt.CA15Puppi_evtNum == count.CA15Puppi_evtNum")
    countmax.createOrReplaceTempView("countmax")
    tdf = spark.sql("SELECT * FROM filteredJets, countmax WHERE filteredJets.CA15Puppi_runNum == countmax.tmp_runNum and filteredJets.CA15Puppi_lumiSec == countmax.tmp_lumiSec and filteredJets.CA15Puppi_pt == countmax.pt").drop("tmp_lumiSec", "tmp_runNum", "tmp_evtNum", "pt")
    tdf
  }

  def createVAddJetDF(sc: SparkContext, spark: SparkSession, dname:String) : DataFrame = {
    import spark.implicits._
    val vaddjet_ds: List[String]= List("AddCA15Puppi.runNum", "AddCA15Puppi.lumisec", "AddCA15Puppi.evtNum", "AddCA15Puppi.tau1", "AddCA15Puppi.tau2", "AddCA15Puppi.tau3", "AddCA15Puppi.mass_sd0", "AddCA15Puppi.sj1_csv", "AddCA15Puppi.sj2_csv", "AddCA15Puppi.sj3_csv",  "AddCA15Puppi.sj4_csv")
    val vaddjet_gn = "/AddCA15Puppi/"
    val vjet_pl = getPartitionInfo(dname, vaddjet_gn+"AddCA15Puppi.tau1")
    val vjet_rdd = sc.parallelize(vjet_pl, vjet_pl.length).flatMap(x=> readDatasets(dname+x.fname, vaddjet_gn, vaddjet_ds, x.begin, x.end))
    var vjet_df = createDataFrame(spark, vjet_rdd, vaddjet_gn, vaddjet_ds)
    vjet_df = vjet_df.withColumn("tau21", taudivUDF($"AddCA15Puppi_tau1", $"AddCA15Puppi_tau2"))
    vjet_df = vjet_df.withColumn("tau32", taudivUDF($"AddCA15Puppi_tau2", $"AddCA15Puppi_tau3"))
    vjet_df = vjet_df.withColumn("mincsv", mincsvUDF($"AddCA15Puppi_sj1_csv", $"AddCA15Puppi_sj2_csv"))
    vjet_df = vjet_df.withColumn("maxsubcsv", maxsubcsvUDF($"AddCA15Puppi_sj1_csv", $"AddCA15Puppi_sj2_csv", $"AddCA15Puppi_sj3_csv", $"AddCA15Puppi_sj4_csv"))
    vjet_df.drop("AddCA15Puppi_tau1", "AddCA15Puppi_tau2", "AddCA15Puppi_tau3", "AddCA15Puppi_sj1_csv", "AddCA15Puppi_sj2_csv", "AddCA15Puppi_sj3_csv", "AddCA15Puppi_sj4_csv")
  }

  def taudivUDF= udf ((tauA:Float, tauB: Float) => tauB/tauA)
  def mincsvUDF = udf ((sj1_csv: Float, sj2_csv: Float) => Math.min(sj1_csv, sj2_csv))
  def maxsubcsvUDF = udf ((sj1_csv: Float, sj2_csv: Float, sj3_csv: Float, sj4_csv: Float) => Math.max(Math.max(sj1_csv, sj2_csv),Math.max(sj3_csv, sj4_csv)))

}

