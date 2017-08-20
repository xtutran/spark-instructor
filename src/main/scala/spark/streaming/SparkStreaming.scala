package spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}

import scala.util.matching.Regex

/**
  * Created by xtutran on 8/7/17.
  */
object SparkStreaming {
  def main(args: Array[String]) {
    // default
    val conf = new SparkConf().setAppName("Problem 1").setMaster("local[2]")

    conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")
    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 7890)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val currentCount = newValues.sum

      val previousCount = runningCount.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val wordCounts = pairs.updateStateByKey[Int](updateFunction _)
    // val wordCounts = pairs.reduceByKey(_ + _)

    // streaming data of bank transaction
    // 1. you need to calculate the number of distinct transaction of each user

    // 2. you need to calulate last transaction of user

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}

