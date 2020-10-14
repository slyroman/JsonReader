
object JsonReader extends  App {
  //println(args.toString)
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)

  import org.apache.spark.sql._

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.testing.memory", "471859200")
    .getOrCreate()

  case class Wine(

                   id: Int,
                   country: String,
                   points: Int,
                   title: String,
                   variety: String,
                   winery: String

                 )

  def sc = spark.sparkContext
  val lines = sc.textFile(args(0).toString)
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats
  lines.map{ row =>
    val json_row = parse(row)
    //.extract[Wine]
    (compact(json_row \ "id"), compact(json_row \ "country"), compact(json_row \ "points"), compact(json_row \ "title"),
      compact(json_row \ "variety"), compact(json_row \ "winery"))
      .toString
  }
    .collect()
    .foreach{println _}


}
