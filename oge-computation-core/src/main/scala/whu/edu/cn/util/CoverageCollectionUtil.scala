package whu.edu.cn.util

import com.typesafe.scalalogging.StrictLogging
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{CellType, MultibandTile, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{RawTile, SpaceTimeBandKey}
import whu.edu.cn.util.CoverageUtil.makeCoverageRDD

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CoverageCollectionUtil extends StrictLogging {
  def checkMapping(coverage: String, algorithm: (String, String, mutable.Map[String, String])): (String, String, mutable.Map[String, String]) = {
    for (tuple <- algorithm._3) {
      if (tuple._2.contains("MAPPING")) {
        algorithm._3.remove(tuple._1)
        algorithm._3.put(tuple._1, coverage)
      }
    }
    algorithm
  }

  def makeCoverageCollectionRDD(rawTileRdd: Map[String, RDD[RawTile]]): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    rawTileRdd.map(t => {
      (t._1, makeCoverageRDD(t._2))
    })
  }

  def coverageCollectionMosaicTemplate(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], method: String):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (coverageCollection.size == 1) {
      return coverageCollection.head._2
    }
    val filteredCoverages = coverageCollection.filter(t => !t._2._1.isEmpty())
    val bandList: List[mutable.ListBuffer[String]] = filteredCoverages.map(t => t._2._1.first()._1.measurementName).toList
    val allSame: Boolean = bandList.forall(_.equals(bandList.head))

    if (allSame && coverageCollection.size != 1) {
      val resoCrsMap: Map[Double, CRS] = coverageCollection.map(t => (t._2._2.cellSize.resolution, t._2._2.crs))
      val resoMin: Double = resoCrsMap.keys.min
      val crs: CRS = resoCrsMap(resoMin)
      val extents = coverageCollection.map(t => t._2._2.extent.reproject(t._2._2.crs, crs)).reduce((e1, e2) => {
        Extent(math.min(e1.xmin, e2.xmin), math.min(e1.ymin, e2.ymin), math.max(e1.xmax, e2.xmax), math.max(e1.ymax, e2.ymax))
      })
      logger.info("coverageCollectionMosaicTemplate current extent", extents)
      val cellType: CellType = coverageCollection.map(t =>
        t._2._1.first()._2.cellType
      ).reduce(_.union(_))


      val layoutCols: Int = math.max(math.ceil((extents.xmax - extents.xmin) / resoMin / 256.0).toInt, 1)
      val layoutRows: Int = math.max(math.ceil((extents.ymax - extents.ymin) / resoMin / 256.0).toInt, 1)

      val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
      // Extent必须进行重新计算，因为layoutCols和layoutRows加了黑边，导致范围变化了
      val newExtent: Extent = new Extent(
        extents.xmin,
        extents.ymin,
        extents.xmin + resoMin * 256.0 * layoutCols,
        extents.ymin + resoMin * 256.0 * layoutRows)
      val ld: LayoutDefinition = LayoutDefinition(newExtent, tl)

      logger.info("执行 List[RDD[(SpatialKey, MultibandTile)]] = coverageCollection.map(coverage ")
      val coverageCollectionRetiled: List[RDD[(SpatialKey, MultibandTile)]] = coverageCollection.map(coverage => {

        val coveragetileRDD: RDD[(SpatialKey, MultibandTile)] = coverage._2._1.map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        val colRowInstant: (Int, Int, Int, Int) = coveragetileRDD.map(t => {
          (t._1._1, t._1._2, t._1._1, t._1._2)
        }).reduce((a, b) => {
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        })
        val newBounds: Bounds[SpatialKey] = Bounds(SpatialKey(colRowInstant._1, colRowInstant._2), SpatialKey(colRowInstant._3, colRowInstant._4))
        val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverage._2._2.cellType, coverage._2._2.layout, coverage._2._2.extent, coverage._2._2.crs, newBounds)
        val coverageRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = MultibandTileLayerRDD(coveragetileRDD, rasterMetaData)

        val (_, reprojectedRdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          coverageRdd.reproject(crs, ld)

        val reprojFilter: RDD[(SpatialKey, MultibandTile)] = reprojectedRdd.filter(layer => {
          val key: SpatialKey = layer._1
          val extentR: Extent = ld.mapTransform(key)
          extentR.xmin >= newExtent.xmin && extentR.xmax <= newExtent.xmax && extentR.ymin >= newExtent.ymin && extentR.ymax <= newExtent.ymax
        })
        reprojFilter
      }).toList

      val time: Long = Instant.now.getEpochSecond

      val rddUnion: RDD[(SpatialKey, MultibandTile)] = coverageCollectionRetiled.reduce((a, b) => {
        a.union(b)
      })
      logger.info("执行DD[(SpatialKey, Iterable[MultibandTile])] = rddUnion.groupByKey() ")
      val groupedRdd: RDD[(SpatialKey, Iterable[MultibandTile])] = rddUnion.groupByKey()
      val coverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = groupedRdd.map(t => {
        (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, time), bandList.head), funcMulti(t._2, method, cellType))
      })

      val spatialKeys: (Int, Int, Int, Int) = coverageRdd.map(t => {
        val spatialKey: SpatialKey = t._1.spaceTimeKey.spatialKey
        (spatialKey.col, spatialKey.row, spatialKey.col, spatialKey.row)
      }).reduce((a, b) => {
        (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
      })

      val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(spatialKeys._1, spatialKeys._2, time), SpaceTimeKey(spatialKeys._3, spatialKeys._4, time))
      val coverageMetadata: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(cellType, ld, newExtent, crs, bounds)
      (coverageRdd, coverageMetadata)
    }
    else {
      throw new IllegalArgumentException("Error: 波段数量不相等，波段的名称、顺序和个数必须完全相同")
    }
  }


  /**
   * 分辨率 和 波段 以及 坐标系都一致
   *
   * @param coverageCollection
   * @return
   */
  def coverageCollectionMosaic2(sc: SparkContext,
                                coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)],
                                  TileLayerMetadata[SpaceTimeKey])],
                                method: String = "mean"):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    // 过滤掉异常导致的RDD[(SpaceTimeBandKey, MultibandTile) 为空的数据

    val layers: List[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = coverageCollection.values.toList
    // 获取第一个图层的元数据作为基础
    // 假设所有图层的 CRS、LayoutDefinition、CellType 兼容
    val layer: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = layers.head
    val firstMetadata = layer._2
    val spatialKeySample = layer._1.first()
    val temporalKey: TemporalKey = spatialKeySample._1.spaceTimeKey.temporalKey
    val bandList: ListBuffer[String] = spatialKeySample._1.measurementName
    val baseCrs: CRS = firstMetadata.crs
    val baseCellType: CellType = firstMetadata.cellType
    val resoMin = firstMetadata.layout.cellSize.resolution

    val extents = layers.map(t => t._2.extent).reduce((e1, e2) => {
      Extent(math.min(e1.xmin, e2.xmin), math.min(e1.ymin, e2.ymin), math.max(e1.xmax, e2.xmax), math.max(e1.ymax, e2.ymax))
    })

    logger.info("coverageCollectionMosaicTemplate current extent", extents)
    // 计算对应布局
    val layoutCols: Int = math.max(math.ceil((extents.xmax - extents.xmin) / resoMin / 256.0).toInt, 1)
    val layoutRows: Int = math.max(math.ceil((extents.ymax - extents.ymin) / resoMin / 256.0).toInt, 1)

    val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
    // Extent必须进行重新计算，因为layoutCols和layoutRows加了黑边，导致范围变化了
    val newExtent = Extent(
      extents.xmin,
      extents.ymin,
      extents.xmin + resoMin * 256 * layoutCols,
      extents.ymin + resoMin * 256 * layoutRows
    )
    val ld = LayoutDefinition(newExtent, tl)

    logger.info("执行 List[RDD[(SpatialKey, MultibandTile)]] = coverageCollection.map(coverage ")
    // 处理每个Coverage
    val reprojectedRDDs = coverageCollection.map { case (_, (rdd, metadata)) =>
      // 直接从元数据获取空间范围，避免RDD操作
      val spatialBounds = metadata.bounds.get
      val minCol = spatialBounds.minKey.spatialKey.col
      val minRow = spatialBounds.minKey.spatialKey.row
      val maxCol = spatialBounds.maxKey.spatialKey.col
      val maxRow = spatialBounds.maxKey.spatialKey.row

      val rasterMeta = TileLayerMetadata(
        metadata.cellType,
        metadata.layout,
        metadata.extent,
        metadata.crs,
        Bounds(
          SpatialKey(minCol, minRow),
          SpatialKey(maxCol, maxRow)
        )
      )

      val spatialRDD = rdd.map { case (key, tile) =>
        (key.spaceTimeKey.spatialKey, tile)
      }

      val coverageRDD = MultibandTileLayerRDD(spatialRDD, rasterMeta)
      val (_, reprojected) = coverageRDD.reproject(baseCrs, ld)

      reprojected.filter { case (key, _) =>
        val extent = ld.mapTransform(key)
        extent.xmin >= newExtent.xmin &&
          extent.xmax <= newExtent.xmax &&
          extent.ymin >= newExtent.ymin &&
          extent.ymax <= newExtent.ymax
      }
    }
    logger.info("执行DD reprojectedRDDs.reduce { (rdd1, rdd2)")
    // 合并时调整分区
    val combinedRDD = sc.union(reprojectedRDDs.toSeq)

    logger.info("执行DD combinedRDD.mapValues")
    // 使用TreeReduce优化聚合
    val finalRDD = combinedRDD
      //      .mapValues(tile => (tile, 1))
      .reduceByKey { case (t1, t2) =>
        funcMulti(Array.apply(t1, t2), method, cellType = baseCellType)
      }
      .map { case (spatialKey, tile) =>
        SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, temporalKey), bandList) -> tile
      }
    logger.info("执行RDD filteredCoverages.flatMap { case (_, (_, metadata))")
    // 构造最终元数据
    val spatialKeys = coverageCollection.flatMap { case (_, (_, metadata)) =>
      val bounds = metadata.bounds.get
      Seq(
        (bounds.minKey.spatialKey.col, bounds.minKey.spatialKey.row),
        (bounds.maxKey.spatialKey.col, bounds.maxKey.spatialKey.row)
      )
    }

    val minCol = spatialKeys.map(_._1).min
    val minRow = spatialKeys.map(_._2).min
    val maxCol = spatialKeys.map(_._1).max
    val maxRow = spatialKeys.map(_._2).max
    val bounds = Bounds(
      SpaceTimeKey(minCol, minRow, temporalKey),
      SpaceTimeKey(maxCol, maxRow, temporalKey)
    )
    val metadata = TileLayerMetadata(baseCellType, ld, newExtent, baseCrs, bounds)
    (finalRDD, metadata)
  }


  def funcMulti(multibandTile: Iterable[MultibandTile], method: String, cellType: CellType): MultibandTile = {
    val flatTiles: Iterable[(Tile, Int)] = multibandTile.flatMap(t => {
      t.bands.zipWithIndex
    })
    val groupedTiles: Map[Int, Iterable[Tile]] = flatTiles.groupBy(t => t._2).map(t => (t._1, t._2.map(x => x._1.convert(cellType))))

    val mapComputed: Map[Int, Tile] =
      method match {
        case "cat" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.Cat(a, b)))
          })
        case "min" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.Min(a, b)))
          })
        case "max" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.Max(a, b)))
          })
        case "sum" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.Add(a, b)))
          })
        case "or" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.OrCollection(a, b)))
          })
        case "and" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.AndCollection(a, b)))
          })
        case "mean" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Mean(a, b)))
          })
        case "median" =>
          //          groupedTiles.map(t => {
          //            val tiles: Iterable[Tile] = t._2
          //            tiles.head.cellType.toString() match {
          //              // 以下为尝试修改部分，若修改正确，可直接将该部分的匹配项添加在"int32" | "int32raw"后面
          //              case "int8" | "int8raw" =>
          //                val bandArrays: Array[Array[Int]] = Array.ofDim[Int](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Int] = tile.toArray()
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val medianValues: Array[Int] = bandArrays.transpose.map(t => {
          //                  if (t.length % 2 == 1) {
          //                    t.sorted.apply(bandArrays.length / 2)
          //                  }
          //                  else {
          //                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2
          //                  }
          //                })
          //                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, medianTile)
          //              //以下为原代码部分
          //              case "int32" | "int32raw" =>
          //                val bandArrays: Array[Array[Int]] = Array.ofDim[Int](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Int] = tile.toArray()
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val medianValues: Array[Int] = bandArrays.transpose.map(t => {
          //                  if (t.length % 2 == 1) {
          //                    t.sorted.apply(bandArrays.length / 2)
          //                  }
          //                  else {
          //                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2
          //                  }
          //                })
          //                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, medianTile)
          //              case "float32" | "float32raw" =>
          //                val bandArrays: Array[Array[Float]] = Array.ofDim[Float](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Float] = tile.toArrayDouble().map(_.toFloat)
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val medianValues: Array[Float] = bandArrays.transpose.map(t => {
          //                  if (t.length % 2 == 1) {
          //                    t.sorted.apply(bandArrays.length / 2)
          //                  }
          //                  else {
          //                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2.0f
          //                  }
          //                })
          //                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, medianTile)
          //              case "float64" | "float64raw" =>
          //                val bandArrays: Array[Array[Double]] = Array.ofDim[Double](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Double] = tile.toArrayDouble()
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val medianValues: Array[Double] = bandArrays.transpose.map(t => {
          //                  if (t.length % 2 == 1) {
          //                    t.sorted.apply(bandArrays.length / 2)
          //                  }
          //                  else {
          //                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2.0
          //                  }
          //                })
          //                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, medianTile)
          //            }
          //          })
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => CoverageOverloadUtil.Median(a, b)))
          })
        case "mode" =>
          //          groupedTiles.map(t => {
          //            val tiles: Iterable[Tile] = t._2
          //            tiles.head.cellType.toString() match {
          //              // 以下为尝试修改部分
          //              case "int8" | "int8raw" =>
          //                val bandArrays: Array[Array[Int]] = Array.ofDim[Int](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Int] = tile.toArray()
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val modeValues: Array[Int] = bandArrays.transpose.map(array => {
          //                  val counts: Map[Int, Int] = array.groupBy(identity).mapValues(_.length)
          //                  val maxCount: Int = counts.values.max
          //                  val modes: List[Int] = counts.filter(_._2 == maxCount).keys.toList
          //                  modes(Random.nextInt(modes.size))
          //                })
          //                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, modeTile)
          //              // 以下为原代码部分
          //              case "int32" | "int32raw" =>
          //                val bandArrays: Array[Array[Int]] = Array.ofDim[Int](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Int] = tile.toArray()
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val modeValues: Array[Int] = bandArrays.transpose.map(array => {
          //                  val counts: Map[Int, Int] = array.groupBy(identity).mapValues(_.length)
          //                  val maxCount: Int = counts.values.max
          //                  val modes: List[Int] = counts.filter(_._2 == maxCount).keys.toList
          //                  modes(Random.nextInt(modes.size))
          //                })
          //                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, modeTile)
          //              case "float32" | "float32raw" =>
          //                val bandArrays: Array[Array[Float]] = Array.ofDim[Float](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Float] = tile.toArrayDouble().map(_.toFloat)
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val modeValues: Array[Float] = bandArrays.transpose.map(array => {
          //                  val counts: Map[Float, Int] = array.groupBy(identity).mapValues(_.length)
          //                  val maxCount: Int = counts.values.max
          //                  val modes: List[Float] = counts.filter(_._2 == maxCount).keys.toList
          //                  modes(Random.nextInt(modes.size))
          //                })
          //                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, modeTile)
          //              case "float64" | "float64raw" =>
          //                val bandArrays: Array[Array[Double]] = Array.ofDim[Double](tiles.size, tiles.head.rows * tiles.head.cols)
          //                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
          //                  val data: Array[Double] = tile.toArrayDouble()
          //                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
          //                }
          //                val modeValues: Array[Double] = bandArrays.transpose.map(array => {
          //                  val counts: Map[Double, Int] = array.groupBy(identity).mapValues(_.length)
          //                  val maxCount: Int = counts.values.max
          //                  val modes: List[Double] = counts.filter(_._2 == maxCount).keys.toList
          //                  modes(Random.nextInt(modes.size))
          //                })
          //                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
          //                (t._1, modeTile)
          //            }
          //          })
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Majority(a, b)))
          })
        case _ =>
          throw new IllegalArgumentException("Error: 该拼接方法不存在:" + method)
      }
    MultibandTile(mapComputed.toArray.sortBy(t => t._1).map(t => t._2))
  }

}
