package fp


import com.google.gson.Gson
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.tsers.zeison.Zeison
import spark.mongodb.MongoDType

/**
  * Created by tran.xuantu on 3/12/2018.
  */
object Example {

  case class ComplexObj(value: (String, Seq[String]))

  def main(args: Array[String]) {

    val global =
      """
        |{"body":{
        |    "method":"string",
        |    "events":"string",
        |    "clients":"string",
        |    "parameter":"string",
        |    "channel":"string",
        |    "metadata":{
        |        "meta1":"string",
        |        "meta2":"string",
        |        "meta3":"string"
        |    }
        |},
        |"timestamp":"string"}
      """.stripMargin


    val f = Map(
      "a" -> Map(
        "b" -> Map(
          "c" -> "d"
        )
      )
    )

    val gson = new Gson()

    println(gson.toJson(f))


    //    API_REQUEST.foreach(f => println(f.getClass))
    //
    //    API_REQUEST map {
    //      case e: String => "String"
    //      case e: ComplexObj => "Tupple2"
    //      case _ => "Any"
    //    } foreach println

  }

}
