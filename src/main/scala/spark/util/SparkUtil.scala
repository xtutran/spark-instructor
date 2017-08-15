package spark.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.functions.when

object SparkUtil {
  type TransformOne = String => Column
  type Merge = (Column, Column) => Column
  type TransformMore[T] = ((String, T)) => Column
  
  def transformOne(features: Seq[String], m: TransformOne, r: Merge) = features.map(m).reduceLeft(r)//.as(alias)
  def transformMore(values: Seq[(String, Any)], m: TransformMore[Any], r: Merge) = values.map(m).reduceLeft(r)//.as(alias)
  def transformMore(dict: Seq[(String, String)], m: TransformMore[String]) = dict.map(m)

  val filter: String => TransformOne = column => prefix => not(col(column).startsWith(prefix))
  val binaryIndex: String => TransformMore[Any] = name => pair => when(col(name).equalTo(pair._1), pair._2)
  val toColumn: TransformOne = column => col(column)
  val and: Merge = (x, y) => x.and(y)
  val or: Merge = (x, y) => x.or(y)
  val otherwise: Merge = (x, y) => x.otherwise(y)
  val plus: Merge = (x, y) => x.plus(y)
  
  val toPositive: TransformMore[String] = pair => when(col(pair._1).<=(0.0), null).otherwise(col(pair._1)).as(pair._2)
  val nullIndex: TransformMore[String] = pair => when(col(pair._1).isNull, 0).otherwise(1).as(pair._2)
  val rename: TransformMore[String] = pair => col(pair._1).as(pair._2)
  val dateNormalize: TransformMore[String] = pair => when(col(pair._1).between("1910-01-01", "2200-01-01"), col(pair._1)).otherwise(null).as(pair._2)
}