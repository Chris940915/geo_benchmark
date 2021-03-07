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
    .appName("readTestScala") // Change this to a proper name
    // Enable GeoSpark custom Kryo serializer
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
  GeoSparkSQLRegistrator.registerAll(sparkSession)

  val resourceFolder = "/geospark/test/"

  var rawDf = sparkSession.read.format("csv").option("header", "false").load(resourceFolder+"real_10m.csv")
  rawDf.createOrReplaceTempView("rawdf")
  print(rawDf.printSchema())

  //Create a Geometry type column in GeoSparkSQL
  var spatialDf = sparkSession.sql(
    """
      |SELECT ST_Point(CAST(rawDf._c0 AS Decimal(24,20)),CAST(rawDf._c1 AS Decimal(24,20))) AS checkin
      |FROM rawDf
    """.stripMargin)

  spatialDf.createOrReplaceTempView("spatialdf")

  spatialDf.show()
  spatialDf.printSchema()

  //Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
  var spatialRDD = new SpatialRDD[Geometry]
  spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)

  val loopTimes = 30

  sparkSession.catalog.clearCache()
  // Box Range
  println("box")

  elapsedTime(Spatial_BoxRangeQuery(1))

  println("------------------")
  println("warm start")
  println("------------------")

  elapsedTime(Spatial_BoxRangeQuery(loopTimes))

  sparkSession.stop()
  println("box 10")

  elapsedTime(Spatial_BoxRange10Query(1))

  println("------------------")
  println("warm start")
  println("------------------")

  elapsedTime(Spatial_BoxRange10Query(loopTimes))

  sparkSession.stop()

  println("box 1000")

  elapsedTime(Spatial_BoxRange1000Query(1))

  println("------------------")
  println("warm start")
  println("------------------")

  elapsedTime(Spatial_BoxRange1000Query(loopTimes))

  sparkSession.stop()

  System.out.println("All GeoSpark DEMOs passed!")


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
      val temp_3 = temp_1 - 1.0
      val temp_4 = temp_2 - 1.0

      val x_ = temp_1*180
      val y_ = temp_2*90
      val min_x_ = temp_3 * 180
      val min_y_ = temp_4 * 90
      
      var sql_query = s"""
                          |SELECT *
                          |FROM spatialDf
                          |WHERE ST_Contains (ST_PolygonFromEnvelope($min_x_,$min_y_, $x_, $y_), checkin)
                        """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.collect()
    }
  }

  def Spatial_BoxRange10Query(x: Int): Unit = {
    val r = scala.util.Random

    for(i <- 1 to x){
      val temp_1 = r.nextFloat
      val temp_2 = r.nextFloat
      val temp_3 = temp_1 - 0.1
      val temp_4 = temp_2 - 0.1

      val x_ = temp_1*180
      val y_ = temp_2*90
      val min_x_ = temp_3 * 180
      val min_y_ = temp_4 * 90

      var sql_query = s"""
                         |SELECT *
                         |FROM spatialDf
                         |WHERE ST_Contains (ST_PolygonFromEnvelope($min_x_,$min_y_, $x_, $y_), checkin)
                        """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.collect()
    }
  }

  def Spatial_BoxRange1000Query(x: Int): Unit = {
    val r = scala.util.Random

    for(i <- 1 to x){
      val temp_1 = r.nextFloat
      val temp_2 = r.nextFloat
      val temp_3 = temp_1 - 9.0
      val temp_4 = temp_2 - 9.0

      val x_ = temp_1*180
      val y_ = temp_2*90
      val min_x_ = temp_3 * 180
      val min_y_ = temp_4 * 90

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