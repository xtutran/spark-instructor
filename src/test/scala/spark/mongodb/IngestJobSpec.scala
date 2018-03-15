package spark.mongodb

import java.io.InputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.bson.Document
import org.scalatest.{Args, FlatSpec, Matchers, Status}

import scala.io.Source

/**
  * Created by tran.xuantu on 3/15/2018.
  */
class IngestJobSpec  extends FlatSpec with Matchers {

  val json = Source.fromInputStream(getClass.getResourceAsStream("/doc_1.json")).getLines().mkString("\n")
  val doc = Document.parse(json)
  val start = IngestJob.dateFormat.parse("20180310000000") //2018-03-10 00:00:00

  "A value" should "be parsed" in {
    IngestJob.get(doc, "_id", MongoDType.OID) shouldBe "5a43d3085ea2c5000fc1eed3"
    IngestJob.get(doc, "createdAt", MongoDType.DATE) shouldBe "20180313002148"
    IngestJob.get(doc, "res.body.message", MongoDType.STRING) shouldBe "registerDate is in invalid format"
    IngestJob.get(doc, "res.body", MongoDType.NAN) shouldBe "{\"message\":\"registerDate is in invalid format\",\"code\":11}"
  }

  it should "be filtered" in {
    IngestJob.filterByTime(doc, start) shouldBe true
  }

  it should "not be filtered" in {
    IngestJob.filterByTime(null, start) shouldBe false
  }

  it should "throw IllegalArgumentException if start=null" in {
    intercept[IllegalArgumentException] {
      IngestJob.filterByTime(doc, null)
    }
  }

}
