package spark.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.{BSON, Document}
import spark.mllib.MainTrait

/**
  * Created by tran.xuantu on 3/7/2018.
  */
object Helper {

  def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = refArrayOps(schema.fields).flatMap(f => {
    val colName = if (prefix == null) f.name else s"$prefix.${f.name}"
    f.dataType match {
      case st: StructType => flattenSchema(st, colName)
      case _ => Array(col(colName))
    }
  })

  val SCHEMA = Seq("_id", "req", "res", "topic", "transporter")
  val REQ_SCHEMA = Seq("headers", "url", "method", "originalUrl", "params", "query", "body")
  val RES_SCHEMA = Seq("headers", "statusCode", "body")

  def flatten(document: Document, schema: Seq[String], prefix: String) = schema.flatMap(key => {
    val value = document.get(key)
    val colName = s"$prefix.$key"
    value match {
      case doc: Document => Map(colName -> doc.toJson)
      case doc => Map(colName -> doc)
    }
  })

  def transform(document: Document) = SCHEMA.flatMap {
    case "req" => flatten(document.get("req", classOf[Document]), REQ_SCHEMA, "req")
    case "res" => flatten(document.get("res", classOf[Document]), RES_SCHEMA, "res")
    case "_id" => Map("_id" -> document.getObjectId("_id").toString)
    case col => Map(col -> document.getString(col))
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val os = System.getProperty("os.name")
    val pwd = System.getProperty("user.dir")
    if (os.startsWith("Windows") && (System.getProperty("hadoop.home.dir") == null))
      System.setProperty("hadoop.home.dir", s"$pwd/hadoop")

    val sc = new SparkContext(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.uri", "mongodb://10.22.250.50:27017/")
      .set("spark.mongodb.input.database", "logs_UAT")
      .set("spark.mongodb.input.collection", "api_request_sample")
      .set("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
      .setMaster("local[*]")
      .setAppName("Spark Job"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

    val rdd = MongoSpark
      .load(sc)
      .map(row => transform(row).toMap)

    val df = rdd.map (value => value.map(t => t._2): _*).toDS().toDF()

    rdd.foreach(println)

//    val df = rdd.toDS().toDF()
//    println(rdd.first())
//    df.show(10)



    //df.limit(1).show()
    //    val sqlContext = new SQLContext(sc)
    //
    //    val df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").load()//MongoSpark.load(sqlContext)
    //    df.printSchema()
    //    df.show(10)
  }
}
