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
    /*GenEvtInfo Group*/
    val genevtinfo_ds: List[String] = List("GenEvtInfo.runNum", "GenEvtInfo.lumisec", "GenEvtInfo.evtNum", "weight", "scalePDF")
    val gname = "/GenEvtInfo/"
    val partitionlist = getPartitionInfo(dname, gname+genevtinfo_ds(0))
    val rdd = sc.parallelize(partitionlist, partitionlist.length).flatMap(x => readDatasets(dname+x.fname, gname, genevtinfo_ds, x.begin, x.end))
    val genevtinfo_df = createDataFrame(spark, rdd, "/GenEvtInfo/", genevtinfo_ds)
    genevtinfo_df.cache()
    println("Num events: " + genevtinfo_df.count())
    var t0 = System.nanoTime()
    val sumWeights =  genevtinfo_df.agg(sum("weight")).first.get(0)
    var t1 = System.nanoTime()
    println("Sum of Weights is: " + sumWeights)
    println("It took :" + (t1 - t0) +" ns to calculate the weight")

     /*Tau DF related operations*/
    val tau_gn = "/Tau/"
    val tau_ds: List[String] = List("Tau.eta", "Tau.pt", "Tau.phi", "Tau.evtNum", "Tau.runNum", "Tau.lumisec", "Tau.hpsDisc", "Tau.rawIso3Hits")
    val tau_pl = getPartitionInfo(dname, tau_gn+"Tau.eta")
    val tau_rdd = sc.parallelize(tau_pl, tau_pl.length).flatMap(x=> readDatasets(dname+x.fname, tau_gn, tau_ds, x.begin, x.end))
    val tau_df = createDataFrame(spark, tau_rdd, tau_gn, tau_ds)
    tau_df.show()
    val tau_fdf = filterTauDF(spark, tau_df)
    val tdf = tau_fdf.groupBy("Tau_evtNum", "Tau_lumisec", "Tau_runNum").max("Tau_pt")
    tdf.show()
    
    /*Photon DF related operations*/
/*    val pho_gn = "/Photon/"
    val pho_ds: List[String] = List("Photon.runNum", "Photon.lumisec", "Photon.evtNum", "Photon.eta", "Photon.pt", "Photon.phi", "Photon.chHadIso", "Photon.scEta", "Photon.neuHadIso", "Photon.gammaIso", "Photon.sieie", "Photon.sthovere")
    val pho_pl = getPartitionInfo(dname, pho_gn+"Photon.eta")
    val pho_rdd = sc.parallelize(pho_pl, pho_pl.length).flatMap(x=> readDatasets(dname+x.fname, pho_gn, pho_ds, x.begin, x.end))
    val pho_df = createDataFrame(spark, pho_rdd, pho_gn, pho_ds)
    pho_df.show()
*/
    /*Info DataFrame*/
    val info_gn = "/Info/"
    val info_ds: List[String] = List("runNum", "lumiSec", "evtNum", "rhoIso", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val info_pl = getPartitionInfo(dname, info_gn+"evtNum")
    val info_rdd = sc.parallelize(info_pl, info_pl.length).flatMap(x=> readDatasets(dname+x.fname, info_gn, info_ds, x.begin, x.end))
    val info_df = createDataFrame(spark, info_rdd, info_gn, info_ds)

    /*Electron DF related operations*/
    val elec_gn = "/Electron/"
    val elec_ds: List[String] = List("Electron.runNum", "Electron.lumisec", "Electron.evtNum", "Electron.eta", "Electron.pt", "Electron.phi", "Electron.chHadIso", "Electron.neuHadIso", "Electron.gammaIso", "Electron.scEta", "Electron.sieie", "Electron.hovere", "Electron.eoverp", "Electron.dEtaIn", "Electron.dPhiIn", "Electron.ecalEnergy", "Electron.d0", "Electron.dz", "Electron.nMissingHits", "Electron.isConv")
    val elec_pl = getPartitionInfo(dname, elec_gn+"Electron.eta")
    val elec_rdd = sc.parallelize(elec_pl, elec_pl.length).flatMap(x=> readDatasets(dname+x.fname, elec_gn, elec_ds, x.begin, x.end))
    val elec_df = createDataFrame(spark, elec_rdd, elec_gn, elec_ds)
    info_df.createOrReplaceTempView("InfoDF")
    elec_df.createOrReplaceTempView("Tdf")
    val ftdf = spark.sql("SELECT * FROM InfoDF, Tdf WHERE InfoDF.evtNum == Tdf.Electron_evtNum and InfoDF.lumiSec == Tdf.Electron_lumisec and infoDF.runNum == Tdf.Electron_runNum").drop("Electron_runNum", "Electron_evtNum", "Electron_lumisec", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    val d = filterElectronDF(spark, ftdf)
    
    /*Photon DF related operations*/
    val pho_gn = "/Photon/"
    val pho_ds: List[String] = List("Photon.runNum", "Photon.lumisec", "Photon.evtNum", "Photon.eta", "Photon.pt", "Photon.phi", "Photon.chHadIso", "Photon.scEta", "Photon.neuHadIso", "Photon.gammaIso", "Photon.sieie", "Photon.sthovere")
    val pho_pl = getPartitionInfo(dname, pho_gn+"Photon.eta")
    val pho_rdd = sc.parallelize(pho_pl, pho_pl.length).flatMap(x=> readDatasets(dname+x.fname, pho_gn, pho_ds, x.begin, x.end))
    val pho_df = createDataFrame(spark, pho_rdd, pho_gn, pho_ds)
    pho_df.show()
    //info_df.createOrReplaceTempView("InfoDF")
    pho_df.createOrReplaceTempView("Tdf")
    val p_df = spark.sql("SELECT * FROM InfoDF, Tdf WHERE InfoDF.evtNum == Tdf.Photon_evtNum and InfoDF.lumiSec == Tdf.Photon_lumisec and infoDF.runNum == Tdf.Photon_runNum").drop("Photon_runNum", "Photon_evtNum", "Photon_lumisec", "metFilterFailBits", "pfMET", "pfMETphi", "puppET", "puppETphi")
    filterPhotonDF(spark, p_df)  //KChHad = 0, KneuHad = 1, Kphoton = 2
    //ftdf.show()


   /*Jet DF Operation*/
/*    val jet_gn = "/AK4Puppi/"
    val jet_ds: List[String] = List("AK4Puppi.runNum", "AK4Puppi.lumisec", "AK4Puppi.evtNum", "AK4Puppi.eta", "AK4Puppi.pt", "AK4Puppi.phi", "AK4Puppi.chHadFrac", "AK4Puppi.chEmFrac", "AK4Puppi.neuHadFrac", "AK4Puppi.neuEmFrac", "AK4Puppi.nParticles", "AK4Puppi.nCharged" )
    val jet_pl = getPartitionInfo(dname, jet_gn+"AK4Puppi.eta")
    val jet_rdd = sc.parallelize(jet_pl, jet_pl.length).flatMap(x=> readDatasets(dname+x.fname, jet_gn, jet_ds, x.begin, x.end))
    val jet_df = createDataFrame(spark, jet_rdd, jet_gn, jet_ds)
    jet_df.show()
    filterJetDF(spark, jet_df)
*/
   /*VJet DF Operation*/
 /*   val vjet_gn = "/CA15Puppi/"
    val vjet_ds: List[String] = List("CA15Puppi.runNum", "CA15Puppi.lumisec", "CA15Puppi.evtNum", "CA15Puppi.eta", "CA15Puppi.pt", "CA15Puppi.phi", "CA15Puppi.chHadFrac", "CA15Puppi.chEmFrac", "CA15Puppi.neuHadFrac", "CA15Puppi.neuEmFrac", "CA15Puppi.nParticles", "CA15Puppi.nCharged" )
    val vjet_pl = getPartitionInfo(dname, vjet_gn+"CA15Puppi.eta")
    val vjet_rdd = sc.parallelize(vjet_pl, vjet_pl.length).flatMap(x=> readDatasets(dname+x.fname, vjet_gn, vjet_ds, x.begin, x.end))
    val vjet_df = createDataFrame(spark, vjet_rdd, vjet_gn, vjet_ds)
    vjet_df.show()
*/
  }
}
