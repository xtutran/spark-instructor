package spark.core

import java.io.BufferedReader

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xtutran on 13/5/17.
  */
object WordCount {
  def main(args: Array[String]) {

    val os = System.getProperty("os.name")
    val pwd = System.getProperty("user.dir")
    if (os.startsWith("Windows") && (System.getProperty("hadoop.home.dir") == null))
      System.setProperty("hadoop.home.dir", s"$pwd/hadoop")

    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
    conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec")

    val sc: SparkContext = new SparkContext(conf)

    // two methods to read data:
    // => read whole file (know block of file, distributed over different node): 10G file , different data node (store partial size of file)
    //worker (data node) => all of block size of file in current worker => memory (RDD)
    //distributed the RDD object over the cluster => combine (somewhere in master node) => fully RDD
    //lazy load
    // whole 100G is not fit to total memomry of cluster (20G first => process => immediate result (hdfs), continue => 20G next =>> ...)
    // till the end you got 100G processed
    // load partial size of file, rest of file will be swap (hdfs => memory)

    //BufferedReader file = new BufferedReader(file)
    //hdfs file: distrubuted over cluster
    //RDD: resilient distributed data
    val textRdd: RDD[String] = sc.textFile("data/wordcount/input/mapred")  //load 100G


    //solve the problem:
    //1. word count: count nb of unique word in input
    //2. How?
    //RDD[String] => ArrayList<String>: for (line in textRdd) { do something } local processing (no hadoop .... )
    // <proccess in distributed>
    // 2.1: RDD object will provide to you 2 types of functions: ...
    // - 1 type: transformation function = map, flatMap    => related to Mapper (in Mapreduce)
    // => map function => string : input => string: output1, string: output2 = flatMap (one string => many string)


    //val wordRdd: RDD[String] = textRdd.flatMap(line => line.split(" "))
    //size(transformedRdd) > size(textRdd)

    //val anotherRdd: RDD[(String, Int)] = wordRdd.map(word => (word, 1))
    //Seq = ArrayList()
    //map to transfor one Rdd[string] to another rdd[String]

    //map: input(one sentence) => output(another sentence with out special chracter)
    //flatMap: input(one sentence) => output(multiple words in the sentence)


    //val transformedRdd2: RDD[String] = textRdd.map(line => line.replace("+", ""))
    //size (transformedRdd2) == size(textRdd)


    // use cases:
    // 1. map
    // - cleaning: filter out some bad thing in your input, for example: RDD[String] (all of sentences in your doc)
    // - numeric character in your sentense: for ex: "Hello, world 123" => hello: 1, world: 1
    // => hello: 1, ,: 1, world: 1, 123: 1
    // => map (input sentence => output sentence without numeric character, special characters)

    // 2. flatMap
    // - split sentence to unique words



    // flatMap, map is trans function convert one RDD to another RDD.

    // => trans func: the function will convert you data from this format to another format.
    // ex: string: ("hello world") => ["hello", "world"] => string.split(" ") => [] <Array>
    // - 2 type: aggregation function = reduceByKey, reduce => related to Reduccer (in Mapred)

    // real example: class: 5 kids: John (age: 4), Mike (age: 3), Mary (age: 5), John: (age 2), John: (age 5)
    // what is the average age of kid who have same name?
    // group class by kid's name, then do calculate the average of age? = aggregation function
    // John (( 4 + 2), 5), Mike (3), Mary(5)

    // reduceByJohn( value1 = 4, value2=2 => 4 + 2 = 6) => (value1 = 6, value = 5 => 6 + 5) => 11

    // csv file, retail transaction data
    // transId, productid, price
    ///.......


    //ask to calculate the median / average / total price of each product
    // => spark job:
    // 1. transactionRdd[String] = sc.textFile("traction....") = csv: trans1, Milk, 20$ ....
    // 2. data: RDD[(String, String, String)] = transactionRdd.map(line => line.split(","))
    // 3. data2: RDD[(String, Double)] = data.map(triple => (triple[1], Double(triple[2])))
    // 4. finalOutput: RDD[(String, Double)] = data2.reduceByKey( value1, value2 => value1 + value2 )
    // 5. finalOutput.write("report")


    //reduceByKey = group by first value of tuple.

    // - 3 type: filter function = new one => filter bad record in your RDD : similar to map
    // filter = map(one input => output: Boolean type)

    //RDD => serializable object
    //textRdd (object): datanode1 (serialized object)

    //textRdd need to be available in datanode2 (copied textRdd => deserialized => RDD), datanode3

    //textRdd take a mount of memory in particular worker

    //hadoop cluster: 1 master node (8G ram), 3 datanode (8G)

    //textRdd store in RAM mem of each node

    //map: one to one transformation (input: one line => output: one line)
    //flatMap: one to many transformation (input: one line => output: mulitple lines)

    //abc  => flat map: abc,  ,

    //map: input: abc => output: tuple (abc, 1)

    //reduceByKey: input: (abc, 1) (abc, 20) (abc, 2) => go to reduce key: list of values: (1, 20) = 1 + 20 = (21, 2) => 23

    //input => transformation (map, flatMap) => reduce (reduceByKey, reduce (summarize result)) => output

    val processRdd: RDD[(String, Int)] = textRdd

      .map(sentence => sentence.replaceAll("[^a-zA-Z]", " "))

      .flatMap(cleanedSentence => cleanedSentence.split(" "))
      //"abc abc" => ["abc", "abc"]

      .filter(singleWord => !singleWord.isEmpty) //! = not
      //.map(singleWord => singleWord.toLowerCase) // hello, Hello => hello: 1, Hello: 1
      //.map(lowercaseWord => (lowercaseWord, 1))
      .map(singleWord => (singleWord.toLowerCase(), 1))
      //
      .reduceByKey((x, y) => x + y) //= group by key

    // processRdd: [(abc, 23), (cde, 2), ....]
    // use map => one2one transform => string "acb\tabcount" => write down to output
    processRdd.map { case(word, count) => s"$word;$count" }.saveAsTextFile("data/wordcount/output/spark")

  }
}
