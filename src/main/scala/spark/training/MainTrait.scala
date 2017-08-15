package spark.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tran.xuantu on 12/7/2016.
  */
trait MainTrait {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val os = System.getProperty("os.name")
    val pwd = System.getProperty("user.dir")
    if (os.startsWith("Windows") && (System.getProperty("hadoop.home.dir") == null))
      System.setProperty("hadoop.home.dir", s"$pwd/hadoop")

    val sc = new SparkContext(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
      .setAppName("Spark Job"))

    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

    createJob(args, sc)
  }

  def readCsv(sqlContext: SQLContext, path: String, header: Boolean = true, sep: String = ","): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", header.toString)
      .option("inferSchema", "true")
      .option("delimiter", sep)
      .load(path)
  }

  def writeCsv(data: DataFrame, path: String, header: Boolean = true, sep: String = ","): Unit = {
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", header.toString)
      .option("inferSchema", "true")
      .option("delimiter", sep)
      .save(path)
  }

  def createJob(args: Array[String], sc: SparkContext)
}
