package spark.core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Hello world!
 *
 */
object HelloSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hello Spark").setMaster("local[*]")
    conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    distData.foreach(println)

  }
}
