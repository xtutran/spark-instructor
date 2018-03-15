package spark.mongodb

/**
  * Created by tran.xuantu on 3/14/2018.
  */
object MongoDType extends Enumeration {
  type MongoDType = Value

  val OID = Value("OID")
  val STRING = Value("STRING")
  val INT = Value("INT")
  val LONG = Value("LONG")
  val DOUBLE = Value("DOUBLE")
  val DATE = Value("TIMESTAMP")
  val BOOL = Value("BOOLEAN")
  val NAN = Value("NAN")
  //val CONFLICT = Value("CONFLICT")

  def withNameOpt(s: String): Option[Value] = values.find(_.toString.equalsIgnoreCase(s))
}
