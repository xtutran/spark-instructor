package spark.mongodb

import java.text.SimpleDateFormat

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import spark.mongodb.MongoDType._

import scala.language.postfixOps

/**
  * Created by tran.xuantu on 3/7/2018.
  */
object IngestJob {

  val TIMESTAMP_COL = "createdAt"
  val gson = new Gson
  val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  def flattenSchema(schema: StructType, prefix: String = null): Array[(String, MongoDType)] = refArrayOps(schema.fields).flatMap(f = f => {
    val colName = if (prefix == null) f.name else s"$prefix.${f.name}"
    (colName, f.dataType) match {
      case ("_id", st: StructType) => Array("_id" -> OID)
      case (_, st: StructType) => flattenSchema(st, colName)
      case (_, t) => Array(colName -> withNameOpt(t.simpleString).getOrElse(NAN))
    }
  })

  def getChild(document: Document, key: String): Document = {
      document.getOrDefault(key, Document.parse("{}")).asInstanceOf[Document]
  }

  def getString(document: Document, key: String, inferType: MongoDType) = {
    val value = document.get(key)
    (value, inferType) match {
      case (null, _) => ""
      case (_, NAN) => gson.toJson(value)
      case (_, OID) => document.getObjectId(key).toString
      case (_, INT | DOUBLE | LONG | BOOL | STRING) => value.toString
      case (_, DATE) => dateFormat.format(document.getDate(key))
      case _ => throw new IllegalArgumentException(s"Not support this type: $inferType")
    }
  }

  def get(document: Document, compositeKey: String, dataType: MongoDType): String = {
    val keys = compositeKey.split("\\.", 2)
    (keys.length, dataType) match {
      case (2, _) => get(getChild(document, keys.head), keys.last, dataType)
      case _ => getString(document, keys.head, dataType)
    }
  }

  def filterByTime(document: Document, start: java.util.Date): Boolean = {
    val current = document.getDate(TIMESTAMP_COL)
    (current, start) match {
      case (_, null) => throw new IllegalArgumentException("start must not be null")
      case (null, _) => false
      case _ => current.compareTo(start) >= 0
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val os = System.getProperty("os.name")
    val pwd = System.getProperty("user.dir")
    if (os.startsWith("Windows") && (System.getProperty("hadoop.home.dir") == null))
      System.setProperty("hadoop.home.dir", s"$pwd/hadoop")

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
      .setAppName("Spark Job")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

    val readConfig = ReadConfig(Map(
      "spark.mongodb.input.uri" -> "",
      "spark.mongodb.input.database" -> "",
      "spark.mongodb.input.collection" -> "",
      "spark.mongodb.input.partitioner" -> "MongoPaginateByCountPartitioner"
    ))

    val writeConfig = WriteConfig(Map(
      "spark.mongodb.output.uri" -> "",
      "spark.mongodb.output.database" -> "",
      "spark.mongodb.output.collection" -> ""
    ))

    val df = MongoSpark.load(sqlContext)
    df.printSchema()

    // infer schema
    val schema = flattenSchema(df.schema) //filterNot(p => p._2.equals("JSON") )
    val struct = StructType(schema.map(p => StructField(p._1, StringType, nullable = true)))
    val toRow = (document: Document) => Row(schema.map(p => get(document, p._1, p._2)): _*)

    // store schema back to mongodb
    /*val schemaRdd = sc.parallelize(List(
      Document.parse(gson.toJson(Map(
        "root" -> schema.map(p => p._1 -> p._2.toString))
      ))
    ))
    MongoSpark.save(schemaRdd, writeConfig)*/

    // extract raw data
    val rdd = MongoSpark.load(sc, readConfig) map toRow
//    filter(filterByTime(_)) map toRow
    val data = sqlContext.createDataFrame(rdd, struct)
    data.printSchema()
    data.show(2)
  }
}
