
import com.mongodb.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
object MainExample extends App {
  println("Hello, world!")

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val mongoUri = "mongodb://127.0.0.1:27017/test.test_2"

  val sparkSession = SparkSession.builder()
    .master("yarn")
    .appName("Geospark_mongodb")
    .config("spark.mongodb.output.uri", mongoUri)
    .config("spark.mongodb.input.uri", mongoUri)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .config("geospark.global.index", "true")
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
  spatialDf.show()

  val loopTimes = 10

  // Cricle
  elapsedTime(Spatial_CircleRangeQuery(1))
  elapsedTime(Spatial_CircleRangeQuery(loopTimes))

  // Box Range
  //elapsedTime(Spatial_BoxRangeQuery(1))
  //elapsedTime(Spatial_BoxRangeQuery(loopTimes))

  // knn
  //elapsedTime(Spatial_KnnQuery(1))
  //elapsedTime(Spatial_KnnQuery(loopTimes))

  // Distance Join
  //elapsedTime(Spatial_DistanceJoin(1))
  //elapsedTime(Spatial_DistanceJoin(loopTimes))

  def elapsedTime[R](block: => R): R = {
    val s = System.currentTimeMillis
    val result = block    // call-by-name
    val e = System.currentTimeMillis
    println("[elapsedTime]: " + ((e - s) / 1000.0f) + " sec")
    result
  }

  // 2, 20, 200, 2000
  def Spatial_BoxRangeQuery(x: Int) = {
    for(i <- 1 to x){
      spatialDf = sparkSession.sql(
        """
          |SELECT *
          |FROM spatialdf
          |WHERE ST_Contains (ST_PolygonFromEnvelope(-180.0,0.0,0.0,136.0), checkin)
          |LIMIT 5
        """.stripMargin)
      spatialDf.createOrReplaceTempView("box_df")
      spatialDf.show()
    }
  }

  // 1, 10, 100, 1000
  def Spatial_CircleRangeQuery(x: Int): Unit = {
    for(i <- 1 to x) {
      spatialDf = sparkSession.sql(
        """
          |SELECT *
          |FROM spatialdf
          |WHERE ST_Distance(ST_Point(1.0,100.0), checkin) < 50
        """.stripMargin)
      spatialDf.createOrReplaceTempView("circle_df")
      spatialDf.show()
      spatialDf.count()
      println(spatialDf.count())
    }
  }

  // 1, 10, 100, 1000
  def Spatial_KnnQuery(x: Int):Unit = {
    for(i <- 1 to x){
      spatialDf = sparkSession.sql(
        """
          |SELECT checkin, ST_Distance(ST_Point(1.0,100.0), checkin) AS distance
          |FROM spatialdf
          |ORDER BY distance DESC
          |LIMIT 5
          """.stripMargin)
      spatialDf.createOrReplaceTempView("knn_df")
      spatialDf.show()
    }
  }

  // 1, 10, 100, 1000
  def Spatial_DistanceJoin(x: Int):Unit ={
    for(i <- 1 to x) {
      spatialDf = sparkSession.sql(
        """
          | SELECT *
          | FROM spatialdf, spatialdf_2
          | WHERE ST_Distance(spatialDf.checkin, spatialDf_2.checkin_2) < 2
      """.stripMargin
      )
      spatialDf.createOrReplaceGlobalTempView("djoin_df")
      spatialDf.show()
    }
  }
}


