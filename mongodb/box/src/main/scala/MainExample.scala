
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

  val mongoUri = "mongodb://172.31.3.155:20000/exp_1_.random_2"

  val sparkSession = SparkSession.builder()
    .master("yarn")
    .appName("Geospark_mongodb")
    .config("spark.mongodb.output.uri", mongoUri)
    .config("spark.mongodb.input.uri", mongoUri)
    .config("spark.mongodb.input.partitioner" ,"MongoShardedPartitioner")
    .config("spark.mongodb.input.localThreshold", "15")
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
  val loopTimes = 50
  spatialDf.show()
  sparkSession.catalog.clearCache()
  println("box")
  elapsedTime(Spatial_BoxRangeQuery(1))
  println("------------------")
  println("warm start")
  println("------------------")
  elapsedTime(Spatial_BoxRangeQuery(loopTimes))


  def elapsedTime[R](block: => R): R = {
    val s = System.currentTimeMillis
    val result = block    // call-by-name
    val e = System.currentTimeMillis
    println("[elapsedTime]: " + ((e - s) / 1000.0f) + " sec")
    result
  }

  def Spatial_BoxRangeQuery(x: Int): Unit = {
    val r = scala.util.Random

    for(i <- 1 to x){

      val temp_1 = r.nextFloat
      val temp_2 = r.nextFloat
      val temp_3 = temp_1 - 0.5
      val temp_4 = temp_2 - 0.5

      val x_ = temp_1*180
      val y_ = temp_2*180
      val min_x_ = temp_3 * 180
      val min_y_ = temp_4 * 180

      var sql_query = s"""
                          |SELECT *
                          |FROM spatialDf
                          |WHERE ST_Contains (ST_PolygonFromEnvelope($min_x_,$min_y_, $x_, $y_), checkin)
                        """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.collect()
    }
  }
}


