#!/usr/bin/env Rscript

suppressPackageStartupMessages(library("optparse"))
suppressPackageStartupMessages(library("lattice"))
suppressPackageStartupMessages(library("latticeExtra"))
suppressPackageStartupMessages(library("tidyr"))
#suppressPackageStartupMessages(library("reshape2"))

make.dataframe <- function()
{
    nodes<-c(8, 16, 32, 64, 128)
    rddutime<-c(2.5, 1.44, 0.89, 0.66, 0.58)
    rddtime<-c(2.8, 1.7, 1.14, 0.88, 0.68)
    dftime<-c(4.28, 3.59, 2.92, 2.6, 2.1)
    dffiltertime<-c(11.77, 7.30, 4.02, 3.07, 2.92)
    df1=data.frame("Nodes"=nodes, "Time"=rddutime, "Step"="RDD Unformatted")
    df2=data.frame("Nodes"=nodes, "Time"=rddtime, "Step"="RDD formatted")
    df3=data.frame("Nodes"=nodes, "Time"=dftime, "Step"="DataFrame")
    df4=data.frame("Nodes"=nodes, "Time"=dffiltertime, "Step"="DataFrame Filtered")
    df.all = rbind(df1, df2, df3, df4)
    df.all$Step=as.factor(df.all$Step)
    df.all
}

format.dataframe <- function() 
{
  df<-make.dataframe()
  newdf=spread(df, key=Step, value=Time)
  newdf$Step1=newdf$"RDD Unformatted"*60
  newdf$Step2=(newdf$"RDD formatted" - newdf$"RDD Unformatted")*60
  newdf$Step3=(newdf$"DataFrame" - newdf$"RDD formatted")*60
  newdf$Step4=(newdf$"DataFrame Filtered" - newdf$"DataFrame")*60
  newdf$Cores=newdf$Nodes*24
  newdf
}
make.plot <- function() {
  df.all<-format.dataframe()
  pdf("scaling.pdf")
  print(xyplot(Step1+Step2+Step3+Step4~Cores, 
               df.all, 
               grid=TRUE,
               type=c("p", "l"), 
               auto.key=list(space="top", columns=4, cex=1.75, fontfamily="mono"), 
               ylab=list("Time for each step (seconds)", cex=1.75, fontfamily="mono"), 
               xlab=list("Number of Cores", cex=1.75, fontfamily="mono"), 
               scales=list(cex=1.75, fontfamily="mono")))
  invisible(dev.off())
}
#make.dataframe()
make.plot()


