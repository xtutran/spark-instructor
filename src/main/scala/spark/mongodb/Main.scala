package spark.mongodb

import scopt.OptionParser


/**
  * Created by tran.xuantu on 3/14/2018.
  */
trait MainTrait {
  case class Config(uri: String = "", commands: Seq[String] = Seq())
  val availableCommand = Set("infer-schema", "check-schema", "ingest-data")

  def parser: OptionParser[Config] = {
    new OptionParser[Config]("iMongo") {
      arg[String]("<command> ...") required() unbounded() action {
        (x, c) => c.copy(commands = c.commands :+ x)
      } validate {
        x => if (availableCommand.contains(x)) success else failure(s"Command $x does not exist")
      } text "command to run"

      note(s"Possible command: ${availableCommand.mkString(",")}\n")

      // list possible parameters
      opt[String]("uri") hidden() action {
        (x, c) => c.copy(uri = x)
      } text "URI of mongodb. Example: mongodb://user:password@host:27017/database.collection"
    }
  }
}

object Main {

}
