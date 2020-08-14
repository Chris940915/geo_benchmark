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

  var rawDf = sparkSession.read.format("csv").option("header", "false").load(resourceFolder+"streets.csv")
  rawDf.createOrReplaceTempView("rawdf")
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

  spatialDf.createOrReplaceTempView("spatialdf")
  spatialDf_2.createOrReplaceTempView("spatialdf_2")

  spatialDf.show()
  spatialDf.printSchema()

  println("------------------")

  spatialDf_2.show()
  spatialDf_2.printSchema()

  //Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
  var spatialRDD = new SpatialRDD[Geometry]
  spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)


//  spatialDf = sparkSession.sql(
//    """
//      |SELECT *
//      |FROM spatialdf
//      |WHERE ST_Intersects (ST_PolygonFromEnvelope(-120.0,33.0, 0.0,35.0), checkin)
//  """.stripMargin)
//  spatialDf.createOrReplaceTempView("spatialdf")
//  spatialDf.show()

  val loopTimes = 5

  //--------------- method ---------------
  //elapsedTime(Spatial_KnnQuery())
  //elapsedTime(Spatial_BoxRangeQuery())
  elapsedTime(Spatial_CircleRangeQuery())
  //elapsedTime(Spatial_DistanceJoin())
  sparkSession.stop()
  System.out.println("All GeoSpark DEMOs passed!")


  def elapsedTime[R](block: => R): R = {
    val s = System.currentTimeMillis
    val result = block    // call-by-name
    val e = System.currentTimeMillis
    println("[elapsedTime]: " + ((e - s) / 1000.0f) + " sec")
    result
  }

  def Spatial_BoxRangeQuery(){
    for(i <- 1 to loopTimes){
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

  def Spatial_CircleRangeQuery() {
    for(i <- 1 to loopTimes) {
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
  // C_NN.
  def Spatial_CNNQuery() {
    for(i <- 1 to loopTimes) {
      spatialDf = sparkSession.sql(
        """
          |SELECT *, ST_Distance(ST_Point(1.0,100.0), checkin) AS distance
          |FROM spatialdf
          |ORDER BY distance DESC
          |WHERE distance < 100
          |LIMIT 5
        """.stripMargin)
      spatialDf.createOrReplaceTempView("cnn_df")
      spatialDf.show()
    }
  }

  8



  /**
   * Test spatial range query.
   *
   * @throws Exception the exception
   */
//  def testSpatialRangeQuery() {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
//    for(i < loopTimes)
//    {
//      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count
//    }
//  }



  /**
   * Test spatial range query using index.
   *
   * @throws Exception the exception
   */
//  def testSpatialRangeQueryUsingIndex() {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    objectRDD.buildIndex(PointRDDIndexType,false)
//    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
//    }
//
//  }

  /**
   * Test spatial knn query.
   *
   * @throws Exception the exception
   */
//  def testSpatialKnnQuery() {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false)
//    }
//  }

  /**
   * Test spatial knn query using index.
   *
   * @throws Exception the exception
   */
//  def testSpatialKnnQueryUsingIndex() {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    objectRDD.buildIndex(PointRDDIndexType,false)
//    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true)
//    }
//  }

  /**
   * Test spatial join query.
   *
   * @throws Exception the exception
   */
//  def testSpatialJoinQuery() {
//    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//
//    objectRDD.spatialPartitioning(joinQueryPartitioningType)
//    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
//
//    objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
//    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count
//    }
//  }

  /**
   * Test spatial join query using index.
   *
   * @throws Exception the exception
   */
//  def testSpatialJoinQueryUsingIndex() {
//    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//
//    objectRDD.spatialPartitioning(joinQueryPartitioningType)
//    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
//
//    objectRDD.buildIndex(PointRDDIndexType,true)
//
//    objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
//    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
//
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count()
//    }
//  }

  /**
   * Test spatial join query.
   *
   * @throws Exception the exception
   */
//  def testDistanceJoinQuery() {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    val queryWindowRDD = new CircleRDD(objectRDD,0.1)
//
//    objectRDD.spatialPartitioning(GridType.QUADTREE)
//    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
//
//    objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
//    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
//
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count()
//    }
//  }

  /**
   * Test spatial join query using index.
   *
   * @throws Exception the exception
   */
//  def testDistanceJoinQueryUsingIndex() {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    val queryWindowRDD = new CircleRDD(objectRDD,0.1)
//
//    objectRDD.spatialPartitioning(GridType.QUADTREE)
//    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
//
//    objectRDD.buildIndex(IndexType.RTREE,true)
//
//    objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
//    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
//
//    for(i <- 1 to eachQueryLoopTimes)
//    {
//      val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count
//    }
//  }
//
//  @throws[Exception]
//  def testCRSTransformationSpatialRangeQuery(): Unit = {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005")
//    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
//    var i = 0
//    while ( {
//      i < eachQueryLoopTimes
//    }) {
//      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
//      assert(resultSize > -1)
//
//      {
//        i += 1; i - 1
//      }
//    }
//  }
//
//
//  @throws[Exception]
//  def testCRSTransformationSpatialRangeQueryUsingIndex(): Unit = {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005")
//    objectRDD.buildIndex(PointRDDIndexType, false)
//    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
//    var i = 0
//    while ( {
//      i < eachQueryLoopTimes
//    }) {
//      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
//      assert(resultSize > -1)
//
//      {
//        i += 1; i - 1
//      }
//    }
//  }
//
//  def testCRSTransformation():Unit =
//  {
//    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
//    // Run Coordinate Reference Systems Transformation on the original data
//    objectRDD.CRSTransform("epsg:4326","epsg:3005")
//    objectRDD.rawSpatialRDD.count()
//  }
//
//  @throws[Exception]
//  def testLoadShapefileIntoPolygonRDD(): Unit = {
//    val spatialRDD = ShapefileReader.readToPolygonRDD(sc, ShapeFileInputLocation)
//    try
//      RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count
//    catch {
//      case e: Exception =>
//        // TODO Auto-generated catch block
//        e.printStackTrace()
//    }
//  }

}