import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}


/**
 * The Class ScalaExample.
 */
object SimpleApp extends App{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession = SparkSession.builder()
    .master("yarn") // Delete this if run in cluster mode
    .appName("hdfs_geospark") // Change this to a proper name
    // Enable GeoSpark custom Kryo serializer
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .config("geospark.global.index", "true")
    .getOrCreate()
  GeoSparkSQLRegistrator.registerAll(sparkSession)

  val resourceFolder = "hdfs://localhost:54311/geospark/test/"

  var rawDf = sparkSession.read.format("csv").option("header", "false").load(resourceFolder+"streets.csv")
  rawDf.createOrReplaceTempView("rawDf")
  print(rawDf.printSchema())

  //Create a Geometry type column in GeoSparkSQL
  var spatialDf = sparkSession.sql(
    """
      |SELECT ST_Point(CAST(rawDf._c0 AS Decimal(24,20)),CAST(rawDf._c1 AS Decimal(24,20))) AS checkin
      |FROM rawDf
    """.stripMargin)

  var spatialDf_2 = sparkSession.sql(
    """
      |SELECT ST_Point(CAST(rawDf._c0 AS Decimal(24,20)),CAST(rawDf._c1 AS Decimal(24,20))) AS checkin_2
      |FROM rawDf
    """.stripMargin)

  spatialDf.createOrReplaceTempView("spatialDf")
  spatialDf_2.createOrReplaceTempView("spatialDf_2")

  spatialDf.show()
  spatialDf.printSchema()

  println("------------------")

  spatialDf_2.show()
  spatialDf_2.printSchema()

  //Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
  var spatialRDD = new SpatialRDD[Geometry]
  spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)

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

  sparkSession.stop()
  System.out.println("All GeoSpark DEMOs passed!")

  def elapsedTime[R](block: => R): R = {
    val s = System.currentTimeMillis
    val result = block    // call-by-name
    val e = System.currentTimeMillis
    println("[elapsedTime]: " + ((e - s) / 1000.0f) + " sec")
    result
  }

  // BoX 2, 20, 200, 2000
  def Spatial_BoxRangeQuery(x: Int): Unit = {
    for(i <- 1 to x){
      spatialDf = sparkSession.sql(
        """
          |SELECT *
          |FROM spatialDf
          |WHERE ST_Contains (ST_PolygonFromEnvelope(-180.0,0.0,0.0,136.0), checkin)
        """.stripMargin)
      spatialDf.createOrReplaceTempView("box_df")
      spatialDf.show()
    }
  }

  // distance : 1, 10, 100, 1000
  def Spatial_CircleRangeQuery(x: Int): Unit = {
    for(i <- 1 to x) {
      spatialDf = sparkSession.sql(
        """
          |SELECT *
          |FROM spatialDf
          |WHERE ST_Distance(ST_Point(-118.0, 62.0), spatialDf.checkin) < 20
        """.stripMargin)
      spatialDf.createOrReplaceTempView("circle_df")
      spatialDf.show()
      spatialDf.count()
      println(spatialDf.count())
    }
  }

  // k :1, 10, 100, 1000
  def Spatial_KnnQuery(x: Int): Unit = {
    for(i <- 1 to x){
      spatialDf = sparkSession.sql(
        """
          |SELECT checkin, ST_Distance(ST_Point(1.0,100.0), checkin) AS distance
          |FROM spatialDf
          |ORDER BY distance DESC
          |LIMIT 100
        """.stripMargin)
      spatialDf.createOrReplaceTempView("knn_df")
      spatialDf.show()
    }
  }

  // distance : 1, 10, 100, 1000
  def Spatial_DistanceJoin(x: Int): Unit = {
    for(i <- 1 to x) {
      spatialDf = sparkSession.sql(
        """
          | SELECT *
          | FROM spatialDf, spatialDf_2
          | WHERE ST_Distance(spatialDf.checkin, spatialDf_2.checkin_2) < 2
      """.stripMargin
      )
      spatialDf.createOrReplaceGlobalTempView("djoin_df")
      spatialDf.show()
    }
  }

}