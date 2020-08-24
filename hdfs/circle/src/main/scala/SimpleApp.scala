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
    .master("local[*]") // Delete this if run in cluster mode
    .appName("readTestScala") // Change this to a proper name
    // Enable GeoSpark custom Kryo serializer
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
  GeoSparkSQLRegistrator.registerAll(sparkSession)

  val resourceFolder = "hdfs://localhost:54311/geospark/test/"

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

  println("------------------")


  val loopTimes = 50

  sparkSession.catalog.clearCache()
  // Box Range
  println("circle")

  elapsedTime(Spatial_CircleRangeQuery(1))
  
  println("------------------")
  println("warm start")
  println("------------------")

  elapsedTime(Spatial_CircleRangeQuery(loopTimes))

  sparkSession.stop()
  System.out.println("All GeoSpark DEMOs passed!")

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

      val x_ = (temp_1-temp_2)*100
      val y_ = (temp_2-temp_3)*100

      var sql_query = s"""
                        |SELECT *
                        |FROM spatialDf
                        |WHERE ST_Distance(ST_Point($x_, $y_), spatialDf.checkin) < 100
                      """
      var spatialDf = sparkSession.sql(sql_query.stripMargin)
      spatialDf.collect()
    }
  }

}