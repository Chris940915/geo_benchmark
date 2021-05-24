
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
    .appName("MongoDB_dj")
    .config("spark.mongodb.output.uri", mongoUri)
    .config("spark.mongodb.input.uri", mongoUri)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()

  val sparkSession_2 = SparkSession.builder()
    .master("yarn")
    .appName("Geospark_mongodb")
    .config("spark.mongodb.output.uri", mongoUri_2)
    .config("spark.mongodb.input.uri", mongoUri_2)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
  
  GeoSparkSQLRegistrator.registerAll(sparkSession)
  GeoSparkSQLRegistrator.registerAll(sparkSession_2)

  case class Character(x: Double, y: Double)

  val rdd = MongoSpark.load[Character](sparkSession)
  val rdd_2 = MongoSpark.load[Character](sparkSession_2)

  val rawDf = rdd.toDF()
  rawDf.createOrReplaceTempView("rawDf")

  val rawDf_2 = rdd_2.toDF()
  rawDf_2.createOrReplaceTempView("rawDf_2")

  var spatialDf = sparkSession.sql(
    """
      |SELECT ST_Point(CAST(rawDf.x AS Decimal(24,20)),CAST(rawDf.y AS Decimal(24,20))) AS checkin
      |FROM rawDf
    """.stripMargin)
  
  var spatialDf_2 = sparkSession_2.sql(
    """
      |SELECT ST_Point(CAST(rawDf_2.x AS Decimal(24,20)),CAST(rawDf_2.y AS Decimal(24,20))) AS checkin_2
      |FROM rawDf_2
    """.stripMargin)

  spatialDf.createOrReplaceTempView("spatialdf")
  spatialDf_2.createOrReplaceTempView("spatialdf_2")

  val loopTimes = 20
  spatialDf.show()

  println("distance join")
  elapsedTime(Spatial_DistanceJoin(1))
  println("------------------")
  println("warm start")
  println("------------------")
  elapsedTime(Spatial_DistanceJoin(loopTimes))

  println("distance 0.1 join")

  elapsedTime(Spatial_DistanceOneJoin(1))
  println("------------------")
  println("warm start")
  println("------------------")
  elapsedTime(Spatial_DistanceOneJoin(loopTimes))

  println("distance 10 join")

  elapsedTime(Spatial_Distance10Join(1))
  println("------------------")
  println("warm start")
  println("------------------")
  elapsedTime(Spatial_Distance10Join(loopTimes))

  def elapsedTime[R](block: => R): R = {
    val s = System.currentTimeMillis
    val result = block    // call-by-name
    val e = System.currentTimeMillis
    println("[elapsedTime]: " + ((e - s) / 1000.0f) + " sec")
    result
  }

  def Spatial_DistanceJoin(x: Int): Unit = {
    for(i <- 1 to x) {
      spatialDf = sparkSession.sql(
        """
          | SELECT *
          | FROM spatialdf, spatialdf_2
          | WHERE ST_Distance(spatialDf.checkin, spatialDf_2.checkin_2) < 1
      """.stripMargin
      )
      spatialDf.collect()
    }
  }


  def Spatial_DistanceOneJoin(x: Int): Unit = {
    for(i <- 1 to x) {

      spatialDf = sparkSession.sql(
        """
          | SELECT *
          | FROM spatialdf, spatialdf_2
          | WHERE ST_Distance(spatialDf.checkin, spatialDf_2.checkin_2) < 0.1
      """.stripMargin
      )
      spatialDf.collect()
    }
  }

  def Spatial_Distance10Join(x: Int): Unit = {
    for(i <- 1 to x) {

      spatialDf = sparkSession.sql(
        """
          | SELECT *
          | FROM spatialdf, spatialdf_2
          | WHERE ST_Distance(spatialDf.checkin, spatialDf_2.checkin_2) < 10
      """.stripMargin
      )
      spatialDf.collect()
    }
  }
}


