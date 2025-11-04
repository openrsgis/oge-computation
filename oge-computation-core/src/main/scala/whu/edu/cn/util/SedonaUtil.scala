package whu.edu.cn.util

import org.apache.log4j.Logger
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.{JoinQuery, KNNQuery, RangeQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.{CircleRDD, SpatialRDD}
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.{Envelope, Geometry}
import whu.edu.cn.oge.OGEGeometry

import java.io.File
import java.util
import scala.collection.JavaConverters._

object SedonaUtil {
  def sedonaSessionBuilder(sc: SparkContext): SparkSession = {
    val conf = sc.getConf
    val sedonaAlreadyConfigured = conf.contains("spark.serializer") || conf.contains("spark.kryo.registrator")
    val sedona = if (sedonaAlreadyConfigured) {
      SparkSession.builder().config(conf).getOrCreate()
    } else {
     SparkSession.builder().config(sc.getConf)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
        .getOrCreate()
    }
    SedonaSQLRegistrator.registerAll(sedona)
    sedona
  }
  //读取Shapefile文件
  def shpToRDD(sc: SparkContext, shpPath: String): SpatialRDD[Geometry] = {
    val rdd: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(sc, s"file://$shpPath") //直接从矢量文件夹读取
    rdd
  }
  def shpToDF(sc: SparkContext, shpPath: String): DataFrame = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val df = Adapter.toDf(shpToRDD(sc, shpPath), spark)
    // 几何列类型GeometryUDT，名称命为"geom"
    df.withColumnRenamed("geometry", "geom")
  }
  //读取GeoJson文件
  def geoJsonToRDD(sc: SparkContext, geoJsonPath: String): SpatialRDD[Geometry] = {
    val rdd: SpatialRDD[Geometry] = GeoJsonReader.readToGeometryRDD(sc, geoJsonPath)
    rdd
  }
  def geoJsonToDF(sc: SparkContext, geoJsonPath: String) = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val df = Adapter.toDf(geoJsonToRDD(sc, geoJsonPath), spark)
    df
  }

  def files2DF(sc: SparkContext, sourceDir: String, fileType: String): DataFrame = {
    val conf = sc.getConf
    val sedonaAlreadyConfigured = conf.contains("spark.serializer") || conf.contains("spark.kryo.registrator")
    val sedona = if (sedonaAlreadyConfigured) {
      SparkSession.builder().config(conf).getOrCreate()
    } else {
      SedonaUtil.sedonaSessionBuilder(sc)
    }
    var unifiedDF: DataFrame = null
    var rdd = new SpatialRDD[Geometry]
    val fileListName = FeatureCollectionUtil.shapeFilesAggregate(new File(sourceDir)).toList
    val dataframes: Seq[DataFrame] = fileListName.par.flatMap { file =>
      val filePath = file.replace("\\", "/")
      try {
        fileType match {
          case "shapefile" => rdd = shpToRDD(sedona.sparkContext, filePath)
          case "GeoJSON" => rdd = geoJsonToRDD(sedona.sparkContext, filePath)
          case _ => throw new IllegalArgumentException(
            s"Unsupported saveType: $fileType. Valid options are: GeoJSON, shapefile")
        }
        var df = Adapter.toDf(rdd, sedona)
        df = df.withColumnRenamed("geometry", "geom")
        Some(df.withColumn("source_file", org.apache.spark.sql.functions.lit(file)))
      } catch {
        case e: Exception =>
          println(s"Error reading $filePath: ${e.getMessage}")
          None
      }
    }.toList

    if (dataframes.nonEmpty) {
      val allColumns = dataframes.flatMap(_.columns).distinct
      val alignedDataFrames = dataframes.map { df =>
        val missingCols = allColumns.diff(df.columns)
        val dfWithMissingCols = missingCols.foldLeft(df) { (tempDF, col) =>
          tempDF.withColumn(col, lit(null))
        }
        dfWithMissingCols.select(allColumns.map(col): _*)
      }
      unifiedDF = alignedDataFrames.reduce(_ union _)
    }
    unifiedDF
  }

  def files2RDD(sc: SparkContext, sourceDir: String, fileType: String): SpatialRDD[Geometry] = {
    val dataFrame = files2DF(sc, sourceDir, fileType)
    val rdd = Adapter.toSpatialRdd(dataFrame, "geom")
    rdd.analyze()
    rdd
  }

  //读取PG数据库数据
  def PG2RDD(sc: SparkContext, query: String): SpatialRDD[Geometry] = {
    val sedona = SedonaUtil.sedonaSessionBuilder(sc)
    val df =
      if (query.take(6).toLowerCase != "select") {
        // Case 1: Load entire table if no query is provided
        PostGisUtil.loadFromPostGIS(sedona, query)
      } else {
        // Case 2: Execute custom query if provided
        PostGisUtil.selectUsingSQL(sedona, query)
      }
    val rdd = Adapter.toSpatialRdd(df, "geom")
    rdd.analyze()
    rdd // Convert DataFrame to SpatialRDD
  }

  def PG2DF(sc: SparkContext, query: String): DataFrame = {
    val sedona = SedonaUtil.sedonaSessionBuilder(sc)
    val df =
      if (query.take(6).toLowerCase != "select") {
        // Case 1: Load entire table if no query is provided
        PostGisUtil.loadFromPostGIS(sedona, query)
      } else {
        // Case 2: Execute custom query if provided
        PostGisUtil.selectUsingSQL(sedona, query)
      }
    df
  }

  // 计算最佳分区数
  private def calculateOptimalPartitionNum(sc: SparkContext, leftCount: Long, rightCount: Long,
                                    // 可配置：期望每分区多少条记录，数据量大的时候可调大一点
                                    targetRecordsPerPartition: Int = 100): Int = {
    // 1. 最大记录数 & Sedona 限制
    val maxCount    = math.max(leftCount, rightCount)
    val minCount    = math.min(leftCount, rightCount)
    val halfCount   = math.max(1, (minCount / 2).toInt)

    // 2. 按数据量算分区数
    val bySize      = math.ceil(maxCount.toDouble / targetRecordsPerPartition).toInt

    // 3. 按集群资源算分区数（通常 defaultParallelism ≈ cores*2）
    val byCores     = sc.defaultParallelism * 2

    // 4. 三者取最小，再保证至少 1 个分区
    val rawPartitions = math.min(math.min(bySize, byCores), halfCount)
    math.max(rawPartitions, 1)
  }

  //范围查询
  def rangeQuery(spatialRDD: SpatialRDD[Geometry], queryGeometry: OGEGeometry, spatialPredicate: String = "Contains", useIndex: Boolean = true, gridType: String = "QUADTREE", indexType: String = "QUADTREE"): SpatialRDD[Geometry] = {
    //要求输入的spatialRDD的成员indexedRawRDD不为空
    //build index
    if (spatialRDD.boundaryEnvelope == null || spatialRDD.approximateTotalCount == -1L) spatialRDD.analyze()
    if (useIndex && spatialRDD.indexedRawRDD == null) {
      indexType match {
        case "QUADTREE" => spatialRDD.buildIndex(IndexType.QUADTREE, false) //RangeQuery.SpatialRangeQuery中取spatialRDD.indexedRawRDD而非spatialRDD.indexedRDD进行操作，因此理论上gridType没有意义
        case "RTREE" => spatialRDD.buildIndex(IndexType.RTREE, false)
      }
    }
    val javaRDD: JavaRDD[Geometry] = spatialPredicate match {
      case "Contains" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.CONTAINS, useIndex)
      case "Intersects" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.INTERSECTS, useIndex)
      case "Within" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.WITHIN, useIndex)
      case "Covers" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.COVERS, useIndex)
      case "CoveredBy" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.COVERED_BY, useIndex)
      case "Touches" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.TOUCHES, useIndex)
      case "Overlaps" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.OVERLAPS, useIndex)
      case "Crosses" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.CROSSES, useIndex)
      case "Equals" => RangeQuery.SpatialRangeQuery(spatialRDD, queryGeometry.geometry, SpatialPredicate.EQUALS, useIndex)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    val resultRDD = new SpatialRDD[Geometry]()
    resultRDD.setRawSpatialRDD(javaRDD)
    resultRDD
  }

  //范围查询(dataFrame支持)
  def rangeQueryDataFrame(dataFrame: DataFrame, queryDataFrame: DataFrame, geometryFieldName: String,
                          spatialPredicate: String = "Contains", properties: String = "geometry"): DataFrame = {
    val num = calculateOptimalPartitionNum(dataFrame.sparkSession.sparkContext, dataFrame.count(), queryDataFrame.count())
    val dataSetPartitioned = ST_GeoHash(dataFrame, geometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val resultDataFrame: DataFrame = spatialPredicate match {
      case "Contains" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.CONTAINS.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Intersects" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.INTERSECTS.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Within" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.WITHIN.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Covers" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.COVERS.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "CoveredBy" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.COVERED_BY.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Touches" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.TOUCHES.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Overlaps" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.OVERLAPS.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Crosses" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.CROSSES.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case "Equals" => ST_Predicate(dataSetPartitioned, queryDataFrame, SpatialPredicate.EQUALS.toString,
        dataSetGeometryFieldName = geometryFieldName, operateGeometryFieldName = geometryFieldName, properties = properties)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    resultDataFrame
  }

  //联合查询
  //SpatialJoin(no spatialPredicate) SpatialJoinQuery<K,List<V>> SpatialJoinQueryFlat<K,V>
  //DistanceJoin DistanceJoinQuery DistanceJoinQueryFlat
  //基于空间关系的联合查询（SpatialJoinQueryFlat）
  def spatialJoinQueryFlat(implicit sc: SparkContext, leftSpatialRDD: SpatialRDD[Geometry], rightSpatialRDD: SpatialRDD[Geometry], useIndex: Boolean = true, spatialPredicate: String = "Contains", gridType: String = "QUADTREE"): JavaPairRDD[Geometry, Geometry] = {
    //JoinQuery.SpatialJoinQuery中有针对useIndex的索引处理策略(默认IndexType.RTREE)，因此这里不再创建索引
    if (leftSpatialRDD.boundaryEnvelope == null || leftSpatialRDD.approximateTotalCount == -1L) leftSpatialRDD.analyze()
    if (rightSpatialRDD.boundaryEnvelope == null || rightSpatialRDD.approximateTotalCount == -1L) rightSpatialRDD.analyze()
//    val numPartitions = math.ceil(math.sqrt(math.max(leftSpatialRDD.approximateTotalCount, rightSpatialRDD.approximateTotalCount).toDouble/5)).toInt
    val numPartitions = calculateOptimalPartitionNum(sc, leftSpatialRDD.approximateTotalCount, rightSpatialRDD.approximateTotalCount)
    if (useIndex && leftSpatialRDD.spatialPartitionedRDD == null) {
      gridType match {
        case "QUADTREE" => leftSpatialRDD.spatialPartitioning(GridType.QUADTREE, numPartitions)
        case "KDBTREE" => leftSpatialRDD.spatialPartitioning(GridType.KDBTREE, numPartitions)
        case "EQUALGRID" => leftSpatialRDD.spatialPartitioning(GridType.EQUALGRID, numPartitions)
      }
    }
    if (useIndex && rightSpatialRDD.spatialPartitionedRDD == null) {
      // leftSpatialRDD
      val spatialPartitioner = leftSpatialRDD.getPartitioner
      // 对rightSpatialRDD应用相同的分区器
      rightSpatialRDD.spatialPartitioning(spatialPartitioner)
    }
    val javaPairRDD: JavaPairRDD[Geometry, Geometry] = spatialPredicate match {
      case "Contains" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.CONTAINS)
      case "Intersects" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.INTERSECTS)
      case "Within" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.WITHIN)
      case "Covers" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.COVERS)
      case "CoveredBy" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.COVERED_BY)
      case "Touches" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.TOUCHES)
      case "Overlaps" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.OVERLAPS)
      case "Crosses" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.CROSSES)
      case "Equals" => JoinQuery.SpatialJoinQueryFlat(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.EQUALS)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    javaPairRDD
  }

  def spatialJoinQueryFlatDataFrame(leftDataFrame: DataFrame, rightDataFrame: DataFrame,
                                    leftGeometryFieldName: String, rightGeometryFieldName: String,
                                    spatialPredicate: String = "Contains"): DataFrame = {
    val num = calculateOptimalPartitionNum(leftDataFrame.sparkSession.sparkContext, leftDataFrame.count(), rightDataFrame.count())
    val leftPartitioned = ST_GeoHash(leftDataFrame, leftGeometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val rightPartitioned = ST_GeoHash(rightDataFrame, rightGeometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val resultDataFrame: DataFrame = spatialPredicate match {
      case "Contains" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.CONTAINS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Intersects" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.INTERSECTS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Within" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.WITHIN.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Covers" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.COVERS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "CoveredBy" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.COVERED_BY.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Touches" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.TOUCHES.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Overlaps" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.OVERLAPS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Crosses" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.CROSSES.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Equals" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.EQUALS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    resultDataFrame
  }
  def spatialJoinQuery(implicit sc: SparkContext, leftSpatialRDD: SpatialRDD[Geometry], rightSpatialRDD: SpatialRDD[Geometry], useIndex: Boolean = true, spatialPredicate: String = "Contains", gridType: String = "QUADTREE"): JavaPairRDD[Geometry, util.List[Geometry]] = {
    //JoinQuery.SpatialJoinQuery中有针对useIndex的索引处理策略(默认IndexType.RTREE)，因此这里不再创建索引
    if (leftSpatialRDD.boundaryEnvelope == null || leftSpatialRDD.approximateTotalCount == -1L) leftSpatialRDD.analyze()
    if (rightSpatialRDD.boundaryEnvelope == null || rightSpatialRDD.approximateTotalCount == -1L) rightSpatialRDD.analyze()
//    val numPartitions = math.ceil(math.sqrt(math.max(leftSpatialRDD.approximateTotalCount, rightSpatialRDD.approximateTotalCount).toDouble/5)).toInt
    val numPartitions = calculateOptimalPartitionNum(sc, leftSpatialRDD.approximateTotalCount, rightSpatialRDD.approximateTotalCount)
    if (useIndex && leftSpatialRDD.spatialPartitionedRDD == null) {
      gridType match {
        case "QUADTREE" => leftSpatialRDD.spatialPartitioning(GridType.QUADTREE, numPartitions)
        case "KDBTREE" => leftSpatialRDD.spatialPartitioning(GridType.KDBTREE, numPartitions)
        case "EQUALGRID" => leftSpatialRDD.spatialPartitioning(GridType.EQUALGRID, numPartitions)
      }
    }
    if (useIndex && rightSpatialRDD.spatialPartitionedRDD == null) {
      // leftSpatialRDD
      val spatialPartitioner = leftSpatialRDD.getPartitioner
      // 对rightSpatialRDD应用相同的分区器
      rightSpatialRDD.spatialPartitioning(spatialPartitioner)
    }
    val javaPairRDD: JavaPairRDD[Geometry, util.List[Geometry]] = spatialPredicate match {
      case "Contains" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.CONTAINS)
      case "Intersects" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.INTERSECTS)
      case "Within" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.WITHIN)
      case "Covers" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.COVERS)
      case "CoveredBy" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.COVERED_BY)
      case "Touches" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.TOUCHES)
      case "Overlaps" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.OVERLAPS)
      case "Crosses" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.CROSSES)
      case "Equals" => JoinQuery.SpatialJoinQuery(leftSpatialRDD, rightSpatialRDD, useIndex, SpatialPredicate.EQUALS)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    javaPairRDD
  }

  def spatialJoinQueryDataFrame(leftDataFrame: DataFrame, rightDataFrame: DataFrame,
                                leftGeometryFieldName: String, rightGeometryFieldName: String,
                                spatialPredicate: String = "Contains"): DataFrame = {
    val num = calculateOptimalPartitionNum(leftDataFrame.sparkSession.sparkContext, leftDataFrame.count(), rightDataFrame.count())
    val leftPartitioned = ST_GeoHash(leftDataFrame, leftGeometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val rightPartitioned = ST_GeoHash(rightDataFrame, rightGeometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val resultDataFrame: DataFrame = spatialPredicate match {
      case "Contains" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.CONTAINS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Intersects" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.INTERSECTS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Within" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.WITHIN.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Covers" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.COVERS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "CoveredBy" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.COVERED_BY.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Touches" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.TOUCHES.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Overlaps" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.OVERLAPS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Crosses" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.CROSSES.toString, leftGeometryFieldName, rightGeometryFieldName)
      case "Equals" => ST_Predicate(leftPartitioned, rightPartitioned, SpatialPredicate.EQUALS.toString, leftGeometryFieldName, rightGeometryFieldName)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    resultDataFrame
      .groupBy(spatialPredicate.toLowerCase + "_geom")
      .agg(
        functions.collect_list(functions.col("source_geom")).as("matched_list")
      )
      .withColumnRenamed(spatialPredicate.toLowerCase + "_geom","query_geom")
  }


  def spatialJoinOneToOne(implicit sc: SparkContext, leftCollection: DataFrame, rightCollection: DataFrame,  leftGeometryFieldName: String = "geom", rightGeometryFieldName: String = "geom", useIndex: Boolean = true, spatialPredicate: String = "Contains", reduceProperties: List[String], reduceRules: List[String], gridType: String = "QUADTREE"): DataFrame = {
    //JoinQuery.SpatialJoinQuery中有针对useIndex的索引处理策略(默认IndexType.RTREE)，因此这里不再创建索引
    val leftSpatialRDD = Adapter.toSpatialRdd(leftCollection, leftGeometryFieldName, leftCollection.columns)
    val rightSpatialRDD = Adapter.toSpatialRdd(rightCollection, rightGeometryFieldName, rightCollection.columns)
    leftSpatialRDD.analyze()
    rightSpatialRDD.analyze()
//    val numPartitions = math.ceil(math.sqrt(math.max(leftSpatialRDD.approximateTotalCount, rightSpatialRDD.approximateTotalCount).toDouble/5)).toInt
    val numPartitions = calculateOptimalPartitionNum(sc, leftSpatialRDD.approximateTotalCount, rightSpatialRDD.approximateTotalCount)
    if (useIndex && leftSpatialRDD.spatialPartitionedRDD == null) {
      gridType match {
        case "QUADTREE" => leftSpatialRDD.spatialPartitioning(GridType.QUADTREE, numPartitions)
        case "KDBTREE" => leftSpatialRDD.spatialPartitioning(GridType.KDBTREE, numPartitions)
        case "EQUALGRID" => leftSpatialRDD.spatialPartitioning(GridType.EQUALGRID, numPartitions)
      }
    }
    if (useIndex && rightSpatialRDD.spatialPartitionedRDD == null) {
      // leftSpatialRDD
      val spatialPartitioner = leftSpatialRDD.getPartitioner
      // 对rightSpatialRDD应用相同的分区器
      rightSpatialRDD.spatialPartitioning(spatialPartitioner)
    }
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    //SedonaUtil中SpatialJoin相关的函数leftCollection和rightCollection与直觉相反
    val javaPairRDD: JavaPairRDD[Geometry, util.List[Geometry]] = spatialPredicate match {
      case "Contains" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.CONTAINS)
      case "Intersects" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.INTERSECTS)
      case "Within" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.WITHIN)
      case "Covers" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.COVERS)
      case "CoveredBy" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.COVERED_BY)
      case "Touches" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.TOUCHES)
      case "Overlaps" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.OVERLAPS)
      case "Crosses" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.CROSSES)
      case "Equals" => JoinQuery.SpatialJoinQuery(rightSpatialRDD, leftSpatialRDD, useIndex, SpatialPredicate.EQUALS)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    if (reduceProperties.size != reduceRules.size) {
      throw new IllegalArgumentException("reduceProperties和reduceRules的长度必须相同")
    }
    // 获取右侧数据集的属性结构
    val rightFields = rightCollection.columns
    val leftSchema :StructType = leftCollection.schema
    // 验证所有reduceProperties都存在于右侧数据集中
    val invalidProps = reduceProperties.filter(!rightFields.contains(_))
    if (invalidProps.nonEmpty) {
      throw new IllegalArgumentException(s"右侧数据集不包含这些属性: ${invalidProps.mkString(", ")}")
    }
    // 将JavaPairRDD转换为RDD[Row]并应用合并规则
    val rightStructType: StructType = StructType(rightCollection.schema.filter(f => f.name != rightGeometryFieldName))
    val leftStructType: StructType = StructType(leftSchema.filter(f=>f.name!=leftGeometryFieldName))
    val leftStructTypeWithGeometry: StructType = StructType(leftSchema.filter(f=>f.name!=leftGeometryFieldName)++leftSchema.filter(f=>f.name==leftGeometryFieldName))
    val rowRDD: RDD[Row] = javaPairRDD.rdd.map { case (leftGeom, rightGeomsList) =>
      // 获取左侧要素的属性
      val leftAttrs: Row = FeatureCollectionUtil.stringToRow(leftGeom.getUserData.toString, leftStructType)
      // 将几何对象添加到属性Row中
      val leftAttrsWithGeometry = new GenericRowWithSchema(
        (leftAttrs.toSeq :+ leftGeom).toArray,
        leftStructTypeWithGeometry
      )
      // 处理右侧要素的属性合并
      val mergedAttrs = reduceProperties.zip(reduceRules).map { case (prop, rule) =>
        // 获取右侧所有匹配要素的该属性值
        val values = rightGeomsList.asScala.map { geom =>
          val row = FeatureCollectionUtil.stringToRow(geom.getUserData.toString, rightStructType)
          row.getAs[Any](prop)
        }.filter(_ != null)  // 过滤null值
        // 根据规则合并值
        rule.toLowerCase match {
          case "first" => if (values.nonEmpty) values.head else null
          case "last" => if (values.nonEmpty) values.last else null
          case "concat" => if (values.nonEmpty) values.mkString(",") else null
          case "sum" => if (values.nonEmpty) values.map(_.asInstanceOf[Number].doubleValue).sum else null
          case "mean" => if (values.nonEmpty) values.map(_.asInstanceOf[Number].doubleValue).sum / values.size else null
          case "median" =>
            if (values.nonEmpty) {
              val sorted = values.map(_.asInstanceOf[Number].doubleValue).sorted
              val mid = sorted.size / 2
              if (sorted.size % 2 == 0) (sorted(mid - 1) + sorted(mid)) / 2 else sorted(mid)
            } else null
          case "mode" =>
            if (values.nonEmpty) {
              values.groupBy(identity).maxBy(_._2.size)._1
            } else null
          case "min" => if (values.nonEmpty) values.map(_.asInstanceOf[Number].doubleValue).min else null
          case "max" => if (values.nonEmpty) values.map(_.asInstanceOf[Number].doubleValue).max else null
          case "std" =>
            if (values.size > 1) {
              val data = values.map(_.asInstanceOf[Number].doubleValue)
              val mean = data.sum / data.size
              math.sqrt(data.map(x => math.pow(x - mean, 2)).sum / (data.size - 1))
            } else null
          case "count" => values.size
          case _ => throw new IllegalArgumentException(s"不支持的合并规则: $rule")
        }
      }
      // 创建合并后的Row
      Row.fromSeq(leftAttrsWithGeometry.toSeq ++ mergedAttrs)
    }
    // 构建合并后的Schema
    val mergedFields = leftStructTypeWithGeometry.fields ++ reduceProperties.zip(reduceRules).map { case (prop, rule) =>
      rule.toLowerCase match {
        case "count" | "sum" | "mean" | "median" | "min" | "max" | "std" =>
          StructField(s"${prop}_${rule}", DoubleType, nullable = true)
        case "concat" =>
          StructField(s"${prop}_${rule}", StringType, nullable = true)
        case "first" | "last" | "mode" =>
          // 使用原始字段类型
          val originalType = rightCollection.schema(prop).dataType
          StructField(s"${prop}_${rule}", originalType, nullable = true)
      }
    }
    val mergedSchema = StructType(mergedFields)
    // 创建DataFrame
    spark.createDataFrame(rowRDD, mergedSchema)
  }

  //基于距离的联合查询
  def distanceJoinQueryFlat(sc: SparkContext, spatialRDD: SpatialRDD[Geometry], queryRDD: SpatialRDD[Geometry],
                            distance: Double = 0.0, useIndex: Boolean = true, spatialPredicate: String = "CoveredBy", gridType: String = "QUADTREE"): DataFrame = {
    //RDD distance joins are only reliable for points. For other geometry types, please use Spatial SQL.
    if (spatialRDD.boundaryEnvelope == null || spatialRDD.approximateTotalCount == -1L) spatialRDD.analyze()
    if (queryRDD.boundaryEnvelope == null || queryRDD.approximateTotalCount == -1L) queryRDD.analyze()
    val numPartitions = calculateOptimalPartitionNum(sc, spatialRDD.approximateTotalCount, queryRDD.approximateTotalCount)
    if (useIndex && spatialRDD.spatialPartitionedRDD == null) {
      gridType match {
        case "QUADTREE" => spatialRDD.spatialPartitioning(GridType.QUADTREE, numPartitions)
        case "KDBTREE" => spatialRDD.spatialPartitioning(GridType.KDBTREE, numPartitions)
        case "EQUALGRID" => spatialRDD.spatialPartitioning(GridType.EQUALGRID, numPartitions)
      }
    }
    if (useIndex && queryRDD.spatialPartitionedRDD == null) {
      // leftSpatialRDD
      val spatialPartitioner = queryRDD.getPartitioner
      // 对queryRDD应用相同的分区器
      queryRDD.spatialPartitioning(spatialPartitioner)
    }
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    if(spatialPredicate != "CoveredBy" && spatialPredicate != "Intersects") throw new IllegalArgumentException("Spatial predicate for distance join should be one of INTERSECTS and COVERED_BY")
    val circleRDD = new CircleRDD(queryRDD, distance) // Create a CircleRDD using the given distance
    val javaPairRDD: JavaPairRDD[Geometry, Geometry] = spatialPredicate match {
      case "Intersects" => JoinQuery.DistanceJoinQueryFlat(spatialRDD, circleRDD, useIndex, SpatialPredicate.INTERSECTS)
      case "CoveredBy" => JoinQuery.DistanceJoinQueryFlat(spatialRDD, circleRDD, useIndex, SpatialPredicate.COVERED_BY)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    Adapter.toDf(javaPairRDD, spark)
  }

  def distanceJoinQuery(sc: SparkContext, spatialRDD: SpatialRDD[Geometry], queryRDD: SpatialRDD[Geometry], distance: Double = 0.0, useIndex: Boolean = true, spatialPredicate: String = "CoveredBy", gridType: String = "QUADTREE"): DataFrame = {
    if (spatialRDD.boundaryEnvelope == null || spatialRDD.approximateTotalCount == -1L) spatialRDD.analyze()
    if (queryRDD.boundaryEnvelope == null || queryRDD.approximateTotalCount == -1L) queryRDD.analyze()
    val numPartitions = calculateOptimalPartitionNum(sc, spatialRDD.approximateTotalCount, queryRDD.approximateTotalCount)
    if (useIndex && spatialRDD.spatialPartitionedRDD == null) {
      gridType match {
        case "QUADTREE" => spatialRDD.spatialPartitioning(GridType.QUADTREE, numPartitions)
        case "KDBTREE" => spatialRDD.spatialPartitioning(GridType.KDBTREE, numPartitions)
        case "EQUALGRID" => spatialRDD.spatialPartitioning(GridType.EQUALGRID, numPartitions)
      }
    }
    if (useIndex && queryRDD.spatialPartitionedRDD == null) {
      val spatialPartitioner = queryRDD.getPartitioner
      queryRDD.spatialPartitioning(spatialPartitioner)
    }
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    if (spatialPredicate != "CoveredBy" && spatialPredicate != "Intersects") throw new IllegalArgumentException("Spatial predicate for distance join should be one of INTERSECTS and COVERED_BY")
    val circleRDD = new CircleRDD(queryRDD, distance)
    val javaPairRDD: JavaPairRDD[Geometry, util.List[Geometry]] = spatialPredicate match {
      case "Intersects" => JoinQuery.DistanceJoinQuery(spatialRDD, circleRDD, useIndex, SpatialPredicate.INTERSECTS)
      case "CoveredBy" => JoinQuery.DistanceJoinQuery(spatialRDD, circleRDD, useIndex, SpatialPredicate.COVERED_BY)
      case _ => throw new IllegalArgumentException(s"不支持的空间谓词: $spatialPredicate")
    }
    val rdd = javaPairRDD.rdd.map { case (geom, geomList) =>
      val listScala = geomList.asScala
      Row(geom, listScala)
    }
    val schema = StructType(Seq(
      StructField("query_geom", GeometryUDT, nullable = false),
      StructField("matched_list", ArrayType(GeometryUDT), nullable = false)
    ))
    spark.createDataFrame(rdd, schema)
  }

  def distanceJoinQueryFlatDataFrame(objectDataFrame: DataFrame, queryDataFrame: DataFrame,
                                     spatialGeometryFieldName: String = "geom", queryGeometryFieldName: String = "geom",
                                     distance: Double = 0.0, limit: String = "less_than"): DataFrame = {
    val num = calculateOptimalPartitionNum(objectDataFrame.sparkSession.sparkContext, objectDataFrame.count(), queryDataFrame.count())
    val dataSetPartitioned = ST_GeoHash(objectDataFrame, spatialGeometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val bufferedDf = queryDataFrame.withColumn("buffer", expr(s"ST_Buffer($queryGeometryFieldName, $distance)"))
    val joined = dataSetPartitioned.join(broadcast(bufferedDf), expr(s"ST_Intersects(buffer, $spatialGeometryFieldName)"))
    val distanceExpr = limit match {
      case "less_than"       => s"ST_Distance(bufferedDf.$queryGeometryFieldName, objectDataFrame.$spatialGeometryFieldName) < $distance"
      case "less_equal"      => s"ST_Distance(bufferedDf.$queryGeometryFieldName, objectDataFrame.$spatialGeometryFieldName) <= $distance"
      case _                 => s"ST_Distance(bufferedDf.$queryGeometryFieldName, objectDataFrame.$spatialGeometryFieldName) < $distance" // 默认行为
    }
    joined
      .filter(expr(distanceExpr))
      .select(
        col(s"bufferedDf.$queryGeometryFieldName").as("query_geom"),
        col(s"objectDataFrame.$spatialGeometryFieldName").as("matched_geom")
      )
  }

  def distanceJoinQueryDataFrame(objectDataFrame: DataFrame, queryDataFrame: DataFrame,
                                 spatialGeometryFieldName: String = "geom", queryGeometryFieldName: String = "geom",
                                     distance: Double = 0.0, limit: String = "less_than"): DataFrame = {
    val num = calculateOptimalPartitionNum(objectDataFrame.sparkSession.sparkContext, objectDataFrame.count(), queryDataFrame.count())
    val dataSetPartitioned = ST_GeoHash(objectDataFrame, spatialGeometryFieldName)
      .repartition(num, col("geohash"))
      .drop("geohash")
    val bufferedDf = queryDataFrame.withColumn("buffer", expr(s"ST_Buffer($queryGeometryFieldName, $distance)"))
    val joined = dataSetPartitioned.join(broadcast(bufferedDf), expr(s"ST_Intersects(buffer, $spatialGeometryFieldName)"))
    val distanceExpr = limit match {
      case "less_than" => s"ST_Distance(bufferedDf.$queryGeometryFieldName, objectDataFrame.$spatialGeometryFieldName) < $distance"
      case "less_equal" => s"ST_Distance(bufferedDf.$queryGeometryFieldName, objectDataFrame.$spatialGeometryFieldName) <= $distance"
      case _ => s"ST_Distance(bufferedDf.$queryGeometryFieldName, objectDataFrame.$spatialGeometryFieldName) < $distance" // 默认行为
    }
    val filteredDf = joined
      .filter(expr(distanceExpr))
      .select(
        col(s"bufferedDf.$queryGeometryFieldName").as("query_geom"),
        col(s"objectDataFrame.$spatialGeometryFieldName").as("matched_geom")
      )
    filteredDf
      .groupBy("query_geom")
      .agg(
        functions.collect_list(functions.col("matched_geom")).as("matched_list")
      )
  }
  //K近邻查询
  //A spatial K Nearest Neighbor query takes as input a K, a query point and a SpatialRDD and finds the K geometries in the RDD which are the closest to the query point.
  def kNNQuery(sc: SparkContext, spatialRDD: SpatialRDD[Geometry], queryPoint: OGEGeometry, k: Int = 10, useIndex: Boolean = true, gridType: String = "QUADTREE", indexType: String = "QUADTREE"): DataFrame ={
    if (spatialRDD.boundaryEnvelope == null || spatialRDD.approximateTotalCount == -1L) spatialRDD.analyze()
    if (useIndex && spatialRDD.spatialPartitionedRDD == null) {
      gridType match {
        case "QUADTREE" => spatialRDD.spatialPartitioning(GridType.QUADTREE)
        case "KDBTREE" => spatialRDD.spatialPartitioning(GridType.KDBTREE)
        case "EQUALGRID" => spatialRDD.spatialPartitioning(GridType.EQUALGRID)
      }
    }
    if (useIndex && spatialRDD.indexedRawRDD == null) {
      indexType match {
        case "QUADTREE" => spatialRDD.buildIndex(IndexType.QUADTREE, true)
        case "RTREE" => spatialRDD.buildIndex(IndexType.RTREE, true)
      }
    }
    val geometryList: util.List[Geometry] = KNNQuery.SpatialKnnQuery(spatialRDD, queryPoint.geometry, k, useIndex)
    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    val resultDataFrame = spark.createDataFrame(geometryList.asScala.map(g => Row(g)).asJava,
      StructType(Seq(StructField("matched_geom", GeometryUDT, nullable = true))))
    //val resultDataFrame = spark.createDataFrame(List(Row.fromSeq(geometryList.toArray)).asJava, StructType(fields))
    resultDataFrame
  }

  def kNNQueryDataFrame(spatialDataFrame: DataFrame, queryPoint: OGEGeometry, k: Int = 10, GeometryFieldName: String = "geom"): DataFrame = {
    val fields: Seq[StructField] = Seq(StructField("geom", GeometryUDT, nullable = true))
    val queryDataFrame = spatialDataFrame.sparkSession.createDataFrame(List(Row.apply(queryPoint)).asJava, StructType(fields))
    val joined = broadcast(queryDataFrame).join(spatialDataFrame)
      .withColumn("distance",expr(s"ST_Distance(queryDataFrame.$GeometryFieldName, spatialDataFrame.$GeometryFieldName)"))
    val kNNDf = joined
      .orderBy(col("distance").asc)
      .limit(k)
      .select(
        col("spatialDataFrame.geom").as("matched_geom")
      )
    kNNDf
  }




  //SpatialRDD.saveAsGeoJSON .saveAsWKB .saveASWKT
  //Persisting calculation results to disk
  def rddSave(
               spatialRDD: SpatialRDD[Geometry],
               saveType: String = "GeoJSON",
               outputLocation: String
             ): Unit = {
    // 参数校验：outputLocation为空时抛出异常
    require(outputLocation != null && outputLocation.nonEmpty,
      "outputLocation must be a non-empty string")

    // 保存逻辑（使用match的穷尽性检查）
    saveType match {
      case "GeoJSON" => spatialRDD.saveAsGeoJSON(outputLocation)
      case "WKB" => spatialRDD.saveAsWKB(outputLocation)
      case "WKT" => spatialRDD.saveAsWKT(outputLocation)
      case _ => throw new IllegalArgumentException(
        s"Unsupported saveType: $saveType. Valid options are: GeoJSON, WKB, WKT")
    }
  }

  /**
   * 分区与索引构建
   *
   * @param indexProperties the properties of index [gridType,indexType]
   */
  def indexBuilder(
                    spatialRDD: SpatialRDD[Geometry],
                    useIndex: Boolean = true,
                    indexProperties: List[String]
                  ): Unit = {
    if (indexProperties.isEmpty)
      throw new NoSuchElementException("Non-indexed properties")
    if (useIndex && spatialRDD.spatialPartitionedRDD == null) {
      indexProperties.head match {
        case "QUADTREE" => spatialRDD.spatialPartitioning(GridType.QUADTREE)
        case "KDBTREE" => spatialRDD.spatialPartitioning(GridType.KDBTREE)
        case "EQUALGRID" => spatialRDD.spatialPartitioning(GridType.EQUALGRID)
        case _ => throw new IllegalArgumentException(
          s"Unsupported saveType: ${indexProperties.head}. Valid options are: QUADTREE, KDBTREE, EQUALGRID")
      }
      if (spatialRDD.indexedRawRDD == null)
        indexProperties(1) match {
          case "QUADTREE" => spatialRDD.buildIndex(IndexType.QUADTREE, true)
          case "RTREE" => spatialRDD.buildIndex(IndexType.RTREE, true)
          case _ => throw new IllegalArgumentException(
            s"Unsupported saveType: ${indexProperties.apply(1)} Valid options are: QUADTREE, RTREE")
        }
    }
  }

  //3.聚合函数(Aggregate Function)
  //3.1 ST_Envelope_Aggr: Return the entire envelope boundary of all geometries in A
  def ST_envelopeAggregate(
                            spatialRDD: SpatialRDD[Geometry]
                          ): Envelope = {
    // 添加空结果保护
    if (spatialRDD.rawSpatialRDD.isEmpty()) {
      return new Envelope(Double.NaN, Double.NaN, Double.NaN, Double.NaN)
    }
    spatialRDD.boundaryEnvelope
  }


  def ST_envelopeAggregate(
                            dataFrame: DataFrame,
                            geomColumnName: String = "geom"
                          ): Geometry = {
    dataFrame
      .selectExpr(s"ST_Envelope_Aggr($geomColumnName) as env")
      .first()
      .getAs[Geometry]("env")
  }

  //通用型工具

  def ST_Property_Set(collection: DataFrame, columnName: String, newContent: Any): DataFrame = {
    collection.withColumn(columnName, functions.lit(newContent))
  }


  //3.2 ST_Intersection_Aggr: Return the polygon intersection of all polygons in A
  def ST_intersectionAggregate(
                                dataFrame: DataFrame,
                                geomColumnName: String = "geom"): Option[OGEGeometry] = {
    require(dataFrame != null, "DataFrame cannot be null")
    require(dataFrame.columns.contains(geomColumnName),
      s"Column '$geomColumnName' not found in DataFrame")
    val logger = Logger.getLogger(getClass)
    try {
      dataFrame.createOrReplaceTempView("dataSet")
      val sedona = dataFrame.sparkSession
      val aggregateDF = sedona.sql("SELECT ST_Intersection_Aggr(geom) FROM dataSet")
      if (aggregateDF.isEmpty) {
        logger.warn("Intersection failed: No result found")
        return None
      }
      val row = aggregateDF.first()
      row.getAs[Any](0) match {
        case null =>
          logger.warn("Intersection result is null")
          None
        case geom: Geometry if geom.isEmpty =>
          logger.warn("Intersection result is an empty geometry")
          None
        case geom: Geometry => Some(new OGEGeometry(geom))
        case other =>
          logger.error(s"Unexpected result type: ${other.getClass.getName}")
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to compute intersection aggregate: ${e.getMessage}", e)
        None
    } finally {
      try {
        dataFrame.sparkSession.catalog.dropTempView("dataSet")
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to drop temp view: ${e.getMessage}")
      }
    }
  }

  //3.3 ST_Union_Aggr: Return the polygon union of all polygons in A 空间合并
  def ST_unionAggregate(sc: SparkContext, dataFrame: DataFrame, geomColumnName: String = "geom"): Option[OGEGeometry] = {
    require(dataFrame != null, "DataFrame cannot be null")
    require(dataFrame.columns.contains(geomColumnName),
      s"Column '$geomColumnName' not found in DataFrame")
    val logger = Logger.getLogger(getClass)
    try {
      val aggregateDF = dataFrame.selectExpr("ST_Union_Aggr(geom)")
      if (aggregateDF.isEmpty) {
        logger.warn("Union failed: No result found")
        return None
      }
      val row = aggregateDF.first()
      row.getAs[Any](0) match {
        case null =>
          logger.warn("Union result is null")
          None
        case geom: Geometry if geom.isEmpty =>
          logger.warn("Union result is an empty geometry")
          None
        case geom: Geometry => Some(new OGEGeometry(geom))
        case other =>
          logger.error(s"Unexpected result type: ${other.getClass.getName}")
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to compute Union aggregate: ${e.getMessage}", e)
        None
    }
  }
  //通用空间分析
  //4.1 计算面要素的面积总和(!!!! 必须都是面要素时 !!!!)
  def ST_AreaAggregate(dataFrame: DataFrame,
                      geomColumnName: String = "geom"): Double = {
    val resultDF = dataFrame.selectExpr(s"ST_Area($geomColumnName) as area")
    resultDF.agg(sum("area")).first().getDouble(0)
  }

  def ST_Area(dataFrame: DataFrame,
                      geomColumnName: String = "geom"): DataFrame = {
    val resultDF =  dataFrame.withColumn("area", expr(s"ST_Area($geomColumnName)"))
    resultDF
  }

  //4.2 计算面要素的周长总和(!! 最好都是面要素，线要素使用length !!)
  def ST_PerimeterAggregate(dataFrame: DataFrame,
                   geomColumnName: String = "geom"): Double = {
    val resultDF = dataFrame.selectExpr(s"ST_Perimeter($geomColumnName) as perimeter")
    resultDF.agg(sum("perimeter")).first().getDouble(0)
  }

  def ST_Perimeter(dataFrame: DataFrame,
                            geomColumnName: String = "geom"): DataFrame = {
    val resultDF =  dataFrame.withColumn("perimeter", expr(s"ST_Perimeter($geomColumnName)"))
    resultDF
  }

  //4.3 计算线要素的长度总和(!!!! 必须都是线要素时 !!!!)
  def ST_LengthAggregate(dataFrame: DataFrame,
                geomColumnName: String = "geom"): Double = {
    val resultDF = dataFrame.selectExpr(s"ST_Length($geomColumnName) as length")
    resultDF.agg(sum("length")).first().getDouble(0)
  }

  def ST_Length(dataFrame: DataFrame,
                         geomColumnName: String = "geom"): DataFrame = {
    val resultDF =
      dataFrame.select(geomColumnName).
        withColumn("length", expr(s"ST_Length($geomColumnName)"))
    resultDF
  }

  //4.4 计算矢量要素的外包围盒
  def ST_Boundary(dataFrame: DataFrame,
                  geomColumnName: String = "geom"): DataFrame = {
    val resultDF =
      dataFrame.
        withColumn("boundary", expr(s"ST_Boundary($geomColumnName)"))
    resultDF
  }

  //4.5 计算矢量要素的质心
  def ST_Centroid(dataFrame: DataFrame,
                  geomColumnName: String = "geom"): DataFrame = {
    val resultDF =
      dataFrame.
        withColumn("centroid", expr(s"ST_Centroid($geomColumnName)"))
    resultDF
  }

  //4.6 缓冲区分析(面向所有几何要素)
  def ST_Buffer(dataFrame: DataFrame,
                geomColumnName: String = "geom",
                buffer: Double, bufferStyleParameters: String = null): DataFrame = {
    val dataFrameWithSRID = dataFrame.withColumn("original_srid", expr(s"ST_SRID($geomColumnName)"))
    val bufferExpr = if (bufferStyleParameters == null)
      s"ST_Buffer($geomColumnName, $buffer)"
    else
      s"ST_Buffer($geomColumnName, $buffer, $bufferStyleParameters)"
    val resultDF = dataFrameWithSRID
      .withColumn(geomColumnName, expr(bufferExpr))
      .withColumn(geomColumnName, expr(s"ST_SetSRID($geomColumnName, original_srid)"))
      .drop("original_srid")
    resultDF
  }

  //4.7 计算矢量要素的geohash值
  def ST_GeoHash(dataFrame: DataFrame,
                 geomColumnName: String = "geom",
                 geoHashPrecision: Int = 10): DataFrame = {
    val resultDF = dataFrame
      .withColumn("geohash", expr(s"ST_GeoHash($geomColumnName, $geoHashPrecision)"))
    resultDF
  }

  //4.8 计算输入几何中顶点集的 Delaunay 三角剖分
  def ST_DelaunayTriangles(dataFrame: DataFrame,
                           geomColumnName: String = "geom"): DataFrame = {
    val resultDF = dataFrame
      .withColumn("DelaunayTriangles", expr(s"ST_DelaunayTriangles($geomColumnName)"))
    resultDF
  }

  //4.9 Performs a DBSCAN clustering across the entire dataframe.
  def ST_DBSCAN(dataFrame: DataFrame,
                epsilon: Double, minPoints: Integer, geomColumnName: String = "geom",
                useSpheroid: Boolean = false
                ): DataFrame = {
    val resultDF = dataFrame
      .withColumn("cluster", expr(s"ST_DBSCAN($geomColumnName, $epsilon, $minPoints, $useSpheroid)"))
    resultDF
  }

  // spatial Predicate analyze
  //5.大规模矢量（多个矢量要素集合）的空间拓扑关系分析与判断
  //5.1 ST_Contains
  //analyze_geometry
  def ST_Contains(
                   DFA: DataFrame,
                   DFB: DataFrame,
                   geometryFieldName_A: String,
                   geometryFieldName_B: String,
                   properties: String = "geometry"
                 ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Contains(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        //当dfb中为进行空间连接操作的基准几何要素，其行数要远小于dfa (下同)
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("contains_geom")
          )
        }
    }
  }

  //5.2 ST_Intersects
  def ST_Intersects(
                     DFA: DataFrame,
                     DFB: DataFrame,
                     geometryFieldName_A: String,
                     geometryFieldName_B: String,
                     properties: String = "geometry"
                   ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Intersects(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("intersects_geom")
          )
        }
    }
  }

  //5.3 ST_Crosses
  def ST_Crosses(
                  DFA: DataFrame,
                  DFB: DataFrame,
                  geometryFieldName_A: String,
                  geometryFieldName_B: String,
                  properties: String = "geometry"
                ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Crosses(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("crosses_geom")
          )
        }
    }
  }

  //5.4 ST_Disjoint
  def ST_Disjoint(
                   DFA: DataFrame,
                   DFB: DataFrame,
                   geometryFieldName_A: String,
                   geometryFieldName_B: String,
                   properties: String = "geometry"
                 ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Disjoint(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("disjoint_geom")
          )
        }
    }
  }

  //5.5 ST_Equals
  def ST_Equals(
                 DFA: DataFrame,
                 DFB: DataFrame,
                 geometryFieldName_A: String,
                 geometryFieldName_B: String,
                 properties: String = "geometry"
               ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Equals(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("equals_geom")
          )
        }
    }
  }

  //5.6 ST_OrderingEquals
  def ST_OrderingEquals(
                         DFA: DataFrame,
                         DFB: DataFrame,
                         geometryFieldName_A: String,
                         geometryFieldName_B: String,
                         properties: String = "geometry"
                       ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_OrderingEquals(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("orderingEquals_geom")
          )
        }
    }
  }

  //5.7 ST_Overlaps
  def ST_Overlaps(
                   DFA: DataFrame,
                   DFB: DataFrame,
                   geometryFieldName_A: String,
                   geometryFieldName_B: String,
                   properties: String = "geometry"
                 ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Overlaps(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("overlaps_geom")
          )
        }
    }
  }

  //5.8 ST_Touches
  def ST_Touches(
                  DFA: DataFrame,
                  DFB: DataFrame,
                  geometryFieldName_A: String,
                  geometryFieldName_B: String,
                  properties: String = "geometry"
                ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Touches(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("touches_geom")
          )
        }
    }
  }

  //5.9 ST_Covers
  def ST_Covers(
                 DFA: DataFrame,
                 DFB: DataFrame,
                 geometryFieldName_A: String,
                 geometryFieldName_B: String,
                 properties: String = "geometry"
               ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Covers(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfB.join(broadcast(dfA), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfB.join(broadcast(dfA), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("covers_geom")
          )
        }
    }
  }

  //5.10 ST_CoveredBy
  def ST_CoveredBy(
                    DFA: DataFrame,
                    DFB: DataFrame,
                    geometryFieldName_A: String,
                    geometryFieldName_B: String,
                    properties: String = "geometry"
                  ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_CoveredBy(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("coveredBy_geom")
          )
        }
    }
  }

  //5.11 ST_Within
  def ST_Within(
                 DFA: DataFrame,
                 DFB: DataFrame,
                 geometryFieldName_A: String,
                 geometryFieldName_B: String,
                 properties: String = "geometry"
               ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Within(b.$geometryFieldName_B, a.$geometryFieldName_A)")
    properties match {
      case "count" =>
        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
      case "geometry" =>
        val bCount = DFB.count()
        val joined = dfA.join(broadcast(dfB), joinExpr)
        if (bCount <= 1) {
          joined.select(s"a.$geometryFieldName_A")
        } else {
          joined.select(
            col(s"a.$geometryFieldName_A").as("source_geom"),
            col(s"b.$geometryFieldName_B").as("within_geom")
          )
        }
    }
  }

  //5.12 ST_DWithin (范围连接查询)1.5.0包不支持
//  def ST_DWithin(
//                  DFA: DataFrame,
//                  DFB: DataFrame,
//                  geometryFieldName_A: String,
//                  geometryFieldName_B: String,
//                  properties: String = "geometry",
//                  distance: Double = 10.0
//                ): DataFrame = {
//    val dfA = DFA.alias("a")
//    val dfB = DFB.alias("b")
//    val joinExpr = expr(s"ST_DWithin(b.$geometryFieldName_B, a.$geometryFieldName_A, $distance)")
//    properties match {
//      case "count" =>
//        dfA.join(broadcast(dfB), joinExpr).selectExpr("count(*) as count")
//      case "geometry" =>
//        val bCount = DFB.count()
//        val joined = dfA.join(broadcast(dfB), joinExpr)
//        if (bCount <= 1) {
//          joined.select(s"a.$geometryFieldName_A")
//        } else {
//          joined.select(
//            col(s"a.$geometryFieldName_A").as("source_geom"),
//            col(s"b.$geometryFieldName_B").as("dwithin_geom")
//          )
//        }
//    }
//  }

  //ALL Predicate in this function
  def ST_Predicate(
                    dataSet: DataFrame,
                    operate: DataFrame,
                    predicateType: String,
                    dataSetGeometryFieldName: String,
                    operateGeometryFieldName: String,
                    properties: String = "geometry"): DataFrame = {
    val result : DataFrame = predicateType.toLowerCase match {
      case "contains" =>
        ST_Contains(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "crosses" =>
        ST_Crosses(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "disjoint" =>
        ST_Disjoint(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "equals" =>
        ST_Equals(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "intersects" =>
        ST_Intersects(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "overlaps" =>
        ST_Overlaps(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "orderingequals" =>
        ST_OrderingEquals(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "touches" =>
        ST_Touches(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "within" =>
        ST_Within(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "covers" =>
        ST_Covers(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case "coveredby" =>
        ST_CoveredBy(dataSet, operate, dataSetGeometryFieldName, operateGeometryFieldName, properties)
      case _ => throw new IllegalArgumentException(
        s"Unsupported sourceType: $properties. Valid options are: Contains, Crosses, Disjoint, " +
          s"Equals, Intersects, Overlaps, OrderingEquals, Touches, Within, Covers, CoveredBy")
    }
    result
  }

  //其他空间分析计算处理
  //5.1 Symmetrical_Difference 计算图层之间的差值
  def ST_Difference(
                     DFA: DataFrame,
                     DFB: DataFrame,
                     geomColumnName1: String = "geom",
                     geomColumnName2: String = "geom"
                   ): DataFrame = {
    val dfA = DFA.alias("a")
    val dfB = DFB.alias("b")
    val joinExpr = expr(s"ST_Intersects(b.$geomColumnName1, a.$geomColumnName2)")
    val resultDF = dfB.join(dfA, joinExpr)
      .select(
        expr(s"ST_Difference(b.$geomColumnName1, a.$geomColumnName2)").alias("geom")
      )
    resultDF
  }

  //5.2 计算图层之间的交集并返回
  def ST_Intersection(
                       DFA: DataFrame,
                       DFB: DataFrame
                     ): DataFrame = {
    require(DFA.columns.contains("geom"), "DFA must contain 'geom' column")
    require(DFB.columns.contains("geom"), "DFB must contain 'geom' column")
    val dfA = DFA.alias("dfa")
    val dfB = DFB.alias("dfb")
    val resultDF = dfA.crossJoin(dfB)
      .where(expr("ST_Intersects(dfa.geom, dfb.geom)"))
      .select(
        expr("ST_Intersection(dfa.geom, dfb.geom)").alias("intersect_geom"),
        col("dfa.geom").alias("geom_a"),
        col("dfb.geom").alias("geom_b")
        // 可加上 .col("dfa.id") 等保留字段
      )
    resultDF
  }

  //5.3 Buffer + Overlay（缓冲区叠加）
  //为图层 A 做缓冲区，然后与图层 B 进行叠加
  def ST_Buffer_Overlay(DFA: DataFrame, DFB: DataFrame, buffer: Double): DataFrame = {
    // Step 1: Buffer 图层 A
    val bufferDF = ST_Buffer(DFA, "geom", buffer)

    // Step 2: Overlay 相交分析
    val resultDF = bufferDF.join(DFB)
      .where(expr("ST_Intersects(buffer, geom)")) // 注意字段名一致
      .select(
        expr("ST_Intersection(buffer, geom)").alias("intersect_geom"),
        bufferDF("buffer").alias("buffer_geom"),
        DFB("geom").alias("geom_b")
      )

    resultDF
  }

  //6 领域分析（邻域分析主要分析的两个要素之间的最短距离，即两个要素彼此之间最接近的距离）
  //6.1 几何距离分析（以点为例子，
  // 几何距离分析指在某一指定搜索半径范围内，确定输入点要素与邻近要素中所有点之间的距离, 生成起点-目的地链接）
  def ST_DomainAnalysis_Distance(DFA: DataFrame, DFB: DataFrame, distance: Double): DataFrame = {
    val dfA = DFA.alias("input")
    val dfB = DFB.alias("des")

    // 将 DFB 广播到所有工作节点
    val resultDF = dfA.join(broadcast(dfB), expr(s"ST_Distance(input.geom, des.geom) <= $distance"))
      .select(
        expr("ST_Distance(input.geom, des.geom)").alias("link_dist"),
        expr("input.geom").alias("geom"),
        expr("des.geom").alias("geom_near")
      )
      .orderBy(col("link_dist").asc)  // 默认升序排序

    resultDF.cache()  // 缓存
    resultDF
  }

  //6.2 近邻分析 (指在搜索半径范围内，确定输入要素中的每个要素与邻近要素中的最近要素之间的距离)
  def ST_DomainAnalysis_Nearest(DFA: DataFrame, DFB: DataFrame, distance: Double): DataFrame = {
    // 调用 ST_domainAnalysis_distance，得到所有的匹配结果及其距离
    val resultDF = ST_DomainAnalysis_Distance(DFA, DFB, distance)
    // 对每个输入要素，选择距离最小的那个邻近要素
    val nearestDF = resultDF
      .groupBy("geom") // 按输入要素的几何列分组
      .agg(
        functions.min("link_dist").alias("min_dist"), // 选择最小距离
        functions.first("geom_near").alias("nearest_geom") // 获取最近邻的几何列（即与最小距离相关的邻近要素）
      )
    nearestDF
  }

  //6.3 面邻域分析 (根据面邻接（重叠、重合边或结点）创建统计数据表)
  def ST_DomainAnalysis_Polygon(sourceDF: DataFrame): DataFrame = {
    // 添加唯一 ID
//    val indexedDF = sourceDF.withColumn("src_index", monotonically_increasing_id())

    // 设置别名
    val dfA = sourceDF.alias("src")
    val dfB = sourceDF.alias("nbr")

    // 查找相交邻域面，排除自身
    val joinedDF = dfA.join(dfB,
      expr("ST_Intersects(src.geom, nbr.geom)") &&
        expr("src.id != nbr.id")
    )

    // 计算空间关系
    val resultDF = joinedDF.select(
      col("src.id").alias("src_id"),
      col("nbr.id").alias("nbr_id"),
      col("src.geom").alias("src_geom"),
      col("nbr.geom").alias("nbr_geom"),
      expr("ST_Intersection(src.geom, nbr.geom)").alias("intersection_geom"),
      expr("ST_Touches(src.geom, nbr.geom)").alias("is_touching"),
      expr("ST_Overlaps(src.geom, nbr.geom)").alias("is_overlapping")
    )
      //如果邻域面是重叠邻域
      //1.计算重叠的面积
      //2.在输出表“面积”字段中记录计算出的面积以供使用
      //3.在输出表“长度”字段中记录 0 以供使用
      //4.在输出表 NODE_COUNT 字段中记录 0 以供使用
      .withColumn("area",
        when(col("is_overlapping"), expr("ST_Area(intersection_geom)")).otherwise(lit(0))
      )
      //如果邻域面是边邻域
      //1.计算重合边界的长度
      //2.在“长度”字段中记录计算出的长度以供使用
      //3.在 NODE_COUNT 字段中记录 0 以供使用
      .withColumn("length",
        when(col("is_touching"), expr("ST_Length(intersection_geom)")).otherwise(lit(0))
      )

    // 统计每个源面与多少个邻域面为“点接触”
    val nodeTouchCounts = resultDF
      .filter(col("area") === 0 && col("length") === 0)
      .groupBy("src_id")
      .agg(functions.count("*").alias("node_count"))

    // 将计数字段加入结果
    val finalResultDF = resultDF
      .join(nodeTouchCounts, Seq("src_id"), "left")
      .withColumn("node_count", coalesce(col("node_count"), lit(0)))
    finalResultDF
  }

  //6.4 创建泰森多边形 (分析)
  def ST_VoronoiPolygons(df: DataFrame, geometryFieldName: String = "geom",
                         tolerance: Double = 0.0, extend_to: Geometry = null): DataFrame = {
    val expr =
      if (tolerance != 0.0 && extend_to != null)
        s"ST_VoronoiPolygons($geometryFieldName, $tolerance, ST_GeomFromWKT('${extend_to.toText}')) as voronoi"
      else
        s"ST_VoronoiPolygons($geometryFieldName) as voronoi"

    df.selectExpr(expr)
  }
}
