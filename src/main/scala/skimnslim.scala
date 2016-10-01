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

    /*Final muon_df after all operations operations*/
    val muon_df = createMuonDF(sc, spark, dname)
    muon_df.write.format("com.databricks.spark.csv").option("header","true").save("muons.csv")

    val info_df = createInfoDF(sc, spark, dname)
    info_df.cache()
    //info_df.show()

    val elec_df = createElectronDF(sc, spark, dname, info_df)
    //elec_df.show()

    val pho_df = createPhotonDF(sc, spark, dname, info_df)
   // pho_df.show()

    val tau_df = createTauDF(sc, spark, dname)
   // tau_df.show()

    val genevtinfo_df = createGenInfoDF(sc, spark, dname)

    val jet_df = createJetDF(sc, spark, dname)
    //jet_df.show()
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
    val newNames = Seq("lumiSec", "runNum", "pt")
    val mdf = fdf.groupBy("Muon_lumisec", "Muon_runNum").max("Muon_pt").toDF(newNames: _*)
    fdf.createOrReplaceTempView("filteredMuons")
    mdf.createOrReplaceTempView("mdf")
    val mdf1 = spark.sql("SELECT * FROM filteredMuons, mdf WHERE filteredMuons.Muon_runNum == mdf.runNum and filteredMuons.Muon_lumiSec == mdf.lumiSec and filteredMuons.Muon_pt == mdf.pt").drop("pt", "runNum", "lumiSec")
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
    val newNames = Seq("lumiSec", "runNum", "pt")
    val tdf = tau_fdf.groupBy("Tau_lumisec", "Tau_runNum").max("Tau_pt").toDF(newNames: _*)
    tau_df.createOrReplaceTempView("TauDF")
    tdf.createOrReplaceTempView("Tdf")
    val ftdf = spark.sql("SELECT * FROM TauDF, Tdf WHERE TauDF.Tau_lumisec == Tdf.lumiSec and TauDF.Tau_runNum == Tdf.runNum and TauDF.Tau_pt == Tdf.pt") .drop("pt", "runNum", "lumiSec")
    ftdf
  }

  def createInfoDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
    val info_gn = "/Info/"
    val info_ds: List[String] = List("runNum", "lumiSec", "evtNum", "rhoIso", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val info_pl = getPartitionInfo(dname, info_gn+"evtNum")
    val info_rdd = sc.parallelize(info_pl, info_pl.length).flatMap(x=> readDatasets(dname+x.fname, info_gn, info_ds, x.begin, x.end))
    val info_df = createDataFrame(spark, info_rdd, info_gn, info_ds)
    println("Number of Events: "+ info_df.count())
    info_df
  }

  def createElectronDF(sc: SparkContext, spark: SparkSession, dname: String, info_df: DataFrame) : DataFrame = {
    val electronMass = 0.000510998910
    val elec_gn = "/Electron/"
    val elec_ds: List[String] = List("Electron.runNum", "Electron.lumisec", "Electron.evtNum", "Electron.eta", "Electron.pt", "Electron.phi", "Electron.chHadIso", "Electron.neuHadIso", "Electron.gammaIso", "Electron.scEta", "Electron.sieie", "Electron.hovere", "Electron.eoverp", "Electron.dEtaIn", "Electron.dPhiIn", "Electron.ecalEnergy", "Electron.d0", "Electron.dz", "Electron.nMissingHits", "Electron.isConv")
    val elec_pl = getPartitionInfo(dname, elec_gn+"Electron.eta")
    val elec_rdd = sc.parallelize(elec_pl, elec_pl.length).flatMap(x=> readDatasets(dname+x.fname, elec_gn, elec_ds, x.begin, x.end))
    val elec_df = createDataFrame(spark, elec_rdd, elec_gn, elec_ds)
   // elec_df.show()
    info_df.createOrReplaceTempView("InfoDF")
    elec_df.createOrReplaceTempView("Tdf")
    val ftdf = spark.sql("SELECT * FROM InfoDF, Tdf WHERE InfoDF.evtNum == Tdf.Electron_evtNum and InfoDF.lumiSec == Tdf.Electron_lumisec and infoDF.runNum == Tdf.Electron_runNum").drop("runNum", "evtNum", "lumiSec", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val fdf = filterElectronDF(spark, ftdf).drop("rhoEffarea", "passfilter1", "passfilter2", "rhoIso")
    val newNames = Seq("lumiSec", "runNum", "pt")
    val mdf = fdf.groupBy("ELectron_lumisec", "Electron_runNum").max("Electron_pt").toDF(newNames: _*)
    fdf.createOrReplaceTempView("filteredElectrons")
    mdf.createOrReplaceTempView("mdf")
    fdf.printSchema
    mdf.printSchema
    val mdf1 = spark.sql("SELECT * FROM filteredElectrons, mdf WHERE filteredElectrons.Electron_runNum == mdf.runNum and filteredElectrons.Electron_lumiSec == mdf.lumiSec and filteredElectrons.Electron_pt == mdf.pt").drop("pt", "runNum", "lumiSec")
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
    val max_tdf = filterPhotonDF(spark, ftdf).groupBy("runNum", "lumiSec").max("Photon_pt").withColumnRenamed("max(Photon_pt)", "Photon_pt")
    val count_tdf = filterPhotonDF(spark, ftdf).groupBy("runNum", "lumiSec").count()
    max_tdf.createOrReplaceTempView("maxpt")
    count_tdf.createOrReplaceTempView("count")
    val tdf = spark.sql("SELECT maxpt.runNum as tmp_runNum, maxpt.lumiSec as tmp_lumiSec, maxpt.Photon_pt, count.count from maxpt, count WHERE maxpt.runNum == count.runNum and maxpt.lumiSec == count.lumiSec")
    ftdf.createOrReplaceTempView("filteredPhotons")
    tdf.createOrReplaceTempView("countmax")
    val tdf1 = spark.sql("SELECT * FROM filteredPhotons, countmax WHERE filteredPhotons.runNum == countmax.tmp_runNum and filteredPhotons.lumiSec == countmax.tmp_lumiSec and filteredPhotons.Photon_pt == countmax.Photon_pt").drop("tmp_lumiSec", "tmp_runNum")
    tdf1
  }

  def createGenInfoDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
       /*Gen Event Info to calculate sum of weights*/
    val genevtinfo_gn = "/GenEvtInfo/"
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val genevtinfo_pl = getPartitionInfo(dname, genevtinfo_gn+"weight")
    val genevtinfo_rdd = sc.parallelize(genevtinfo_pl, genevtinfo_pl.length).flatMap(x=> readDatasets(dname+x.fname, genevtinfo_gn, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createDataFrame(spark, genevtinfo_rdd, genevtinfo_gn, genevtinfo_ds)
    genevtinfo_df.cache()
    println("Num events: " + genevtinfo_df.count())
    var t0 = System.nanoTime()
    val sumWeights =  genevtinfo_df.agg(sum("weight")).first.get(0)
    var t1 = System.nanoTime()
    println("Sum of Weights is: " + sumWeights)
    println("It took :" + (t1 - t0) +" ns to calculate the weight")
    genevtinfo_df
  }
  
  def createJetDF(sc: SparkContext, spark: SparkSession, dname: String) : DataFrame = {
    val jet_gn = "/AK4Puppi/"
    val jet_ds: List[String] = List("AK4Puppi.runNum", "AK4Puppi.lumisec", "AK4Puppi.evtNum", "AK4Puppi.eta", "AK4Puppi.pt", "AK4Puppi.phi", "AK4Puppi.chHadFrac", "AK4Puppi.chEmFrac", "AK4Puppi.neuHadFrac", "AK4Puppi.neuEmFrac", "AK4Puppi.nParticles", "AK4Puppi.nCharged" )
    val jet_pl = getPartitionInfo(dname, jet_gn+"AK4Puppi.eta")
    val jet_rdd = sc.parallelize(jet_pl, jet_pl.length).flatMap(x=> readDatasets(dname+x.fname, jet_gn, jet_ds, x.begin, x.end))
    val jet_df = createDataFrame(spark, jet_rdd, jet_gn, jet_ds)
    filterJetDF(spark, jet_df).drop("passfilter")
  }

}

