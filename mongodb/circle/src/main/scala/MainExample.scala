
import com.mongodb.spark._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Envelope}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
object MainExample extends App {
  println("Hello, world!")

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  val sparkSession = SparkSession.builder()
    .master("yarn")
    .appName("MongoDB_cirlce")
    .config("spark.mongodb.output.uri", mongoUri)
    .config("spark.mongodb.input.uri", mongoUri)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
  GeoSparkSQLRegistrator.registerAll(sparkSession)

  case class Character(x: Double, y: Double)

  val rdd = MongoSpark.load[Character](sparkSession)
  val rawDf = rdd.toDF()
  rawDf.createOrReplaceTempView("rawDf")
  println(rawDf.printSchema())

  var spatialDf = sparkSession.sql(
    """
      |SELECT ST_Point(CAST(rawDf.x AS Decimal(24,20)),CAST(rawDf.y AS Decimal(24,20))) AS checkin
      |FROM rawDf
    """.stripMargin)

  spatialDf.createOrReplaceTempView("spatialdf")
  val loopTimes = 20
  spatialDf.show()

  sparkSession.catalog.clearCache()
  // Box Range
  println("circle")
  elapsedTime(Spatial_CircleRangeQuery(1))
  println("------------------")
  println("warm start")
  println("------------------")  
  elapsedTime(Spatial_CircleRangeQuery(loopTimes))
  sparkSession.stop()

  println("circle 10")
  elapsedTime(Spatial_CircleRange10Query(1))
  println("------------------")
  println("warm start")
  println("------------------")
  elapsedTime(Spatial_CircleRange10Query(loopTimes))
  sparkSession.stop()

  println("circle 1000")
  elapsedTime(Spatial_CircleRange1000Query(1))
  println("------------------")
  println("warm start")
  println("------------------")
  elapsedTime(Spatial_CircleRange1000Query(loopTimes))
  sparkSession.stop()

  def elapsedTime[R](block: => R): R = {
    val s = System.currentTimeMillis
    val result = block    // call-by-name
    val e = System.currentTimeMillis
    println("[elapsedTime]: " + ((e - s) / 1000.0f) + " sec")
    result
  }

  def Spatial_CircleRangeQuery(x: Int): Unit = {
    val r = scala.util.Random

    for(i <- 1 to x) {
      val temp_1 = r.nextFloat
      val temp_2 = r.nextFloat
      val temp_3 = r.nextFloat

      val x_ = (temp_1-temp_2)*180
      val y_ = (temp_2-temp_3)*90

      var sql_query = s"""
                        |SELECT *
                        |FROM spatialDf
                        |WHERE ST_Distance(ST_Point($x_, $y_), spatialDf.checkin) < 100
                      """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.show()
    }
  }


  def Spatial_CircleRange10Query(x: Int): Unit = {
    val r = scala.util.Random

    for(i <- 1 to x) {
      val temp_1 = r.nextFloat
      val temp_2 = r.nextFloat
      val temp_3 = r.nextFloat

      val x_ = (temp_1-temp_2)*180
      val y_ = (temp_2-temp_3)*90

      var sql_query = s"""
                         |SELECT *
                         |FROM spatialDf
                         |WHERE ST_Distance(ST_Point($x_, $y_), spatialDf.checkin) < 10
                      """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.collect()
    }
  }

  def Spatial_CircleRange1000Query(x: Int): Unit = {
    val r = scala.util.Random

    for(i <- 1 to x) {
      val temp_1 = r.nextFloat
      val temp_2 = r.nextFloat
      val temp_3 = r.nextFloat

      val x_ = (temp_1-temp_2)*180
      val y_ = (temp_2-temp_3)*90

      var sql_query = s"""
                         |SELECT *
                         |FROM spatialDf
                         |WHERE ST_Distance(ST_Point($x_, $y_), spatialDf.checkin) < 1000
                      """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.collect()
    }
  }

}


