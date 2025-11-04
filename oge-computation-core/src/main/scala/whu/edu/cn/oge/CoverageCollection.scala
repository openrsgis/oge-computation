package whu.edu.cn.oge

import com.typesafe.scalalogging.StrictLogging
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, MultibandTile, TileLayout}
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.{geom => jts}
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity._
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.CoverageCollectionUtil.{coverageCollectionMosaic2, coverageCollectionMosaicTemplate}
import whu.edu.cn.util.CoverageUtil.{calculateLayout, makeCoverageRDD}
import whu.edu.cn.util.{COGUtil, CRSUtils, ClientUtil, PostgresqlServiceUtil}

import java.time.ZoneOffset
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer

object CoverageCollection extends StrictLogging {
  /**
   * load the images
   *
   * @param sc              spark context
   * @param productName     product name to query
   * @param sensorName      sensor name to query
   * @param measurementName measurement name to query
   * @param dateTime        array of start time and end time to query
   * @param geom            geom of the query window
   * @param crs             crs of the images to query
   * @return ((RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile])
   */
  def load(implicit sc: SparkContext,
           productName: String,
           sensorName: String = null,
           measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String],
           startTime: String = null,
           endTime: String = null,
           extent: Extent = null,
           crs: CRS = null,
           level: Int = 0,
           cloudCoverMin: Float = 0,
           cloudCoverMax: Float = 100,
           customFiles: ArrayBuffer[String] = ArrayBuffer.empty[String],
           bands: ArrayBuffer[String] = ArrayBuffer.empty[String],
           coverageIDList: ArrayBuffer[String] = ArrayBuffer.empty[String]
          ): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {

    val union = extent
    val metaList: List[CoverageMetadata] =
      PostgresqlServiceUtil.queryCoverageCollection(
        productName,
        sensorName,
        measurementName,
        startTime,
        endTime,
        if (union != null) extent.toPolygon() else null,
        crs,
        cloudCoverMin,
        cloudCoverMax,
        bands,
        coverageIDList).toList
    val cogUtil: COGUtil = COGUtil.createCOGUtil(CLIENT_NAME)
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    Trigger.windowExtent = extent

    val tileMetadata: Map[String, List[CoverageMetadata]] = metaList.groupBy(t => t.getCoverageID)
    var coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] =
      tileMetadata.flatMap { case (key, metaListCoverage) =>
      logger.info("key: " + key + " " + metaListCoverage.size)
      // 同一个CoverageID 下的数据
      val tileDataRdd: RDD[CoverageMetadata] = sc.parallelize(metaListCoverage, Math.min(metaListCoverage.size * 2, 24))
      // 构造  RDD[RawTile]
      val rawTileRdd: RDD[RawTile] = tileDataRdd.mapPartitions { metadataIterator =>
        metadataIterator.flatMap { metadata =>
          val client = clientUtil.getClientPool
          try {
            val startTime = System.currentTimeMillis()
            val tiles = cogUtil.tileQuery(
              client,
              level,
              metadata,
              if (union != null) Extent(union.getEnvelopeInternal) else null,
              metadata
                .getGeom)
            logger.info(s"Get Tiles Meta Time: ${System.currentTimeMillis() - startTime} ms," +
              s"key : ${metadata.getCoverageID}, file path : ${metadata.getPath}, tile num : ${tiles.size}")
            tiles
          } catch {
            case e: Exception =>
              logger.error("Failed to fetch tiles", e)
              Iterator.empty
          } finally {
            clientUtil.returnClient(client)
          }
        }
      }.persist(StorageLevel.DISK_ONLY)

      val tileCount = rawTileRdd.count().toInt
      logger.info(s"Tile count for $key: $tileCount")

      if (tileCount > 0) {
        // 合并 extents 和 colRowInstant 计算
        val initialMinMax = rawTileRdd.map(t => {
          val extent = t.getExtent
          val spatialKey = t.getSpatialKey
          val time = t.getTime.toEpochSecond(ZoneOffset.ofHours(0))
          ((extent.xmin, extent.ymin, extent.xmax, extent.ymax),
            (spatialKey.col, spatialKey.row, time, spatialKey.col, spatialKey.row, time))
        })
        // 构造数据 extent  和
        val ((xmin, ymin, xmax, ymax), (colMin, rowMin, timeMin, colMax, rowMax, timeMax)) =
          initialMinMax.reduce { (a, b) =>
          val (extentA, colRowTimeA) = a
          val (extentB, colRowTimeB) = b
          ((math.min(extentA._1, extentB._1),
            math.min(extentA._2, extentB._2),
            math.max(extentA._3, extentB._3), math.max(extentA._4, extentB._4)),
            (math.min(colRowTimeA._1, colRowTimeB._1), math.min(colRowTimeA._2, colRowTimeB._2),
              math.min(colRowTimeA._3, colRowTimeB._3), math.max(colRowTimeA._4, colRowTimeB._4),
              math.max(colRowTimeA._5, colRowTimeB._5), math.max(colRowTimeA._6, colRowTimeB._6)))
        }
        val extents = (xmin, ymin, xmax, ymax)
        val colRowInstant = (colMin, rowMin, timeMin, colMax, rowMax, timeMax)

        // 优化 measurementResoMap
        val measurementResoMap: Map[String, Double] = rawTileRdd.aggregate(Map.empty[String, Double])(
          (map, tile) => map + (tile.getMeasurement -> tile.getResolutionCol),
          (map1, map2) => map1 ++ map2
        )
        val resoMax = measurementResoMap.values.max

        // 初始化 metadata
        val firstTile = rawTileRdd.first()
        val (layoutCols, layoutRows) = calculateLayout(extents, resoMax)
        val extent = Extent(extents._1, extents._2, extents._1 + layoutCols * resoMax * 256.0, extents._2 + layoutRows * resoMax * 256.0)
        val tl = TileLayout(layoutCols, layoutRows, 256, 256)
        val ld = LayoutDefinition(extent, tl)
        val cellType = CellType.fromName(firstTile.getDataType.toString)
        val crs = firstTile.getCrs
        val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
        val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)

        /**
         * 利用原有的 rawTileRdd 缓存来计算元数据相关信息包括 尺寸分辨率,在需要触发获取具体 tileBuf的时候才 repartition,保证数据分发均匀
         * 一般来说分区数在100-100000之间，取决于集群的大小和数据
         * 下限 - cores的2倍
         * 上限 - 每个task的执行时间最好在100+ms
         *
         * 需要根据 tileCount 数量来动态分区,否则任务会堆积
         */
        val rawTileRddPer = tileCount match {
          case t if t > 100000 => rawTileRdd.repartition(math.max(100, tileCount / 1000))
          case t if t >= 10000 && t < 100000 => rawTileRdd.repartition(32)
          case _ => rawTileRdd
        }
        rawTileRddPer.persist(StorageLevel.DISK_ONLY)
        val coverageRdd: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeCoverageRDD(rawTileRddPer, colRowInstant, tileLayerMetadata, measurementResoMap, extents, extent, cellType, crs)
        rawTileRdd.unpersist()
        rawTileRddPer.unpersist()
        Some(key -> coverageRdd)
      } else {
        rawTileRdd.unpersist()
        None
      }

    }.toMap

    if (customFiles.nonEmpty) {
      coverageCollection = coverageCollection ++ loadCustomFiles(sc, customFiles)
    }
    coverageCollection
  }

  private def loadCustomFiles(sc: SparkContext, customFiles: Seq[String]): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    customFiles.flatMap { file =>
      try {
        val rasterData = Coverage.loadCoverageFromUpload(sc, file, Trigger.userId, Trigger.dagId)
        Some((file, rasterData))
      } catch {
        case e: Exception =>
          logger.error(s"Error loading custom file $file", e)
          None
      }
    }.toMap
  }


  def mergeCoverages(coverages: List[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], names: List[String]): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    if (coverages.length != names.length) {
      throw new Exception("Coverages 和 Names数量不匹配！")
    }
    names.zip(coverages).toMap
  }

  def mosaic(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "mean")
  }

  def mosaic(sc: SparkContext,
             coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
             method: String):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) =
      if (method != null)
        coverageCollectionMosaic2(sc, coverageCollection, method)
      else
        coverageCollectionMosaic2(sc, coverageCollection)

    val coverageExtend = coverage._2.extent
    val reprojectedExtent = Coverage.getIntersectionExtend(coverageExtend, coverage._2.crs)
    if (reprojectedExtent.isDefined) {
      import geotrellis.vector._
      val geometry: jts.Geometry = reprojectedExtent.get
      Coverage.clip(coverage, geometry.extent)
    } else {
      coverage
    }
  }

  def mean(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "mean")
  }

  def min(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "min")
  }

  def max(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "max")
  }

  def sum(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "sum")
  }

  def or(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "or")
  }

  def and(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "and")
  }

  def median(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "median")
  }

  def mode(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "mode")
  }

  def cat(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "cat")
  }

  def filter(filter: String, collection: CoverageCollectionMetadata): CoverageCollectionMetadata = {
    val newCollection: CoverageCollectionMetadata = collection
    val filterGet: (String, mutable.Map[String, String]) = Trigger.lazyFunc(filter)
    Filter.func(newCollection, filter, filterGet._1, filterGet._2)
    newCollection
  }

  def visualizeOnTheFly(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], visParam: VisualizationParam): Unit = {
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(coverageCollection)
    COGUtil.extent = coverage._2.extent
    Coverage.visualizeOnTheFly(sc, coverage, visParam)
  }

  def visualizeBatch(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): Unit = {
  }

}
