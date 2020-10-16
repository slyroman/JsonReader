
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

                   id: Option[Int] = None,
                   country: Option[String]= None,
                   points: Option[Int]= None,
                   title: Option[String]= None,
                   variety: Option[String]= None,
                   winery: Option[String]= None

                 )

  def sc = spark.sparkContext
  val lines = sc.textFile(args(0).toString)

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats

    lines
    .foreach(line => println(
      parse(line).extract[Wine].toString
    ))
    //.collect().foreach(println)

}
