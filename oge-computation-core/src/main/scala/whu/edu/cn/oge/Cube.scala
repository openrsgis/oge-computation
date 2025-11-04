package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import com.baidubce.services.bos.BosClient
import com.baidubce.services.bos.model.{BosObjectSummary, ListObjectsResponse}
import com.typesafe.scalalogging.StrictLogging
import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local.{Add, Divide, Multiply, Subtract}
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile, Raster, Tile, TileLayout}
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.Extent
import io.minio.messages.Item
import io.minio.{ListObjectsArgs, MinioClient, Result}
import geotrellis.raster.{reproject => _, _}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import javafx.scene.paint.Color
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.MinioConf.MINIO_HEAD_SIZE
import whu.edu.cn.entity.{CoverageMetadata, VisualizationParam}
import whu.edu.cn.entity.cube._
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.{COGUtil, CoverageUtils, PostSender}
import whu.edu.cn.util.cube.CubePostgresqlUtil.{loadCubeSubsetJointByImage, loadCubeSubsetJointByTile}
import whu.edu.cn.util.cube.CubeUtil.cubeTemplate
//import whu.edu.cn.util.BosClientUtil_scala.getBosObject

import java.lang
import java.sql.ResultSet
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.Console.println
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{max, min}
import scala.util.matching.Regex

object Cube extends StrictLogging {

  def loadCubeByImage(implicit sc: SparkContext, cubeId: String, productString: String, bandString: String, timeString: String, extentString: String, tms: String, resolution: Double): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    val product: Array[String] = productString.substring(1, productString.length - 1).split(",")
    val band: Array[String] = bandString.substring(1, bandString.length - 1).split(",")
    val time: Array[String] = timeString.substring(1, timeString.length - 1).split(",")
    val extent: Array[Double] = extentString.substring(1, extentString.length - 1).split(",").map(_.toDouble)
    loadCubeSubsetJointByImage(sc, cubeId, product, band, time, extent(0), extent(1), extent(2), extent(3), tms, resolution)
  }

  def bandRadiometricCalibration(cubeRDD: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], gain: Double, offset: Double): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    cubeRDD.map(rdd => (rdd._1.map(t => {
      val tile: Tile = t._2.convert(DoubleConstantNoDataCellType)
      val newTile: Tile = tile.mapDouble(x => x * gain + offset)
      (t._1, newTile)
    }), rdd._2))
  }

  def normalizedDifference(implicit sc: SparkContext, cubeRDD: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], bandName1: String, platform1: String, bandName2: String, platform2: String): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    val bandKey1: BandKey = new BandKey(bandName1, platform1)
    val bandKey2: BandKey = new BandKey(bandName2, platform2)
    var ndvicube: ArrayBuffer[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = ArrayBuffer()
    var platformNew: String = ""
    if (platform1.equals(platform2)) {
      platformNew = platform1
    } else {
      platformNew = platform1 + "_" + platform2
    }
    val cubeProcess1: Iterable[(CubeTileKey, Tile)] = cubeRDD.filter(t => t._1.first()._1.bandKey.equals(bandKey1) || t._1.first()._1.bandKey.equals(bandKey2)).flatMap(_._1.collect()).toIterable
    val cubeProcess2: RDD[((String, Int, Int), Iterable[(CubeTileKey, Tile)])] = sc.parallelize(cubeProcess1.groupBy(t => (t._1.spaceKey.tms, t._1.spaceKey.col, t._1.spaceKey.row)).toSeq)
    val cubeProcess3: (RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey]) = (cubeProcess2.map(rdd => {
      val band1: Tile = rdd._2.filter(_._1.bandKey.equals(bandKey1)).map(_._2).localMean().convert(DoubleConstantNoDataCellType)
      val band2: Tile = rdd._2.filter(_._1.bandKey.equals(bandKey2)).map(_._2).localMean().convert(DoubleConstantNoDataCellType)
      val normalizedDifferenceTile: Tile = band1.localSubtract(band2).localDivide(band1.localAdd(band2))
      (new CubeTileKey(rdd._2.head._1.spaceKey, rdd._2.head._1.timeKey, rdd._2.head._1.productKey, new BandKey("NDVI", platformNew)), normalizedDifferenceTile)
    }), cubeRDD.head._2)
    ndvicube.append(cubeProcess3)
    ndvicube.toArray
  }


  def visualization(cubeRDD: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]): Unit = {
    cubeRDD.foreach(t => {
      val productName: String = t._1.collect().head._1.productKey.productName
      val productType: String = t._1.collect().head._1.productKey.productType
      val bandName: String = t._1.collect().head._1.bandKey.bandName
      val bandPlatform: String = t._1.collect().head._1.bandKey.bandPlatform
      val timestampSeconds: Long = t._1.collect().head._1.timeKey.time
      val time: String = LocalDateTime.ofEpochSecond(timestampSeconds, 0, ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      val coverageArray: Array[(SpatialKey, Tile)] = t._1.collect().map(rdd => (new SpatialKey(rdd._1.spaceKey.col, rdd._1.spaceKey.row), rdd._2))
      val extentTuple4: (Double, Double, Double, Double) = t._1.map(x => (x._1.spaceKey.minX, x._1.spaceKey.minY, x._1.spaceKey.maxX, x._1.spaceKey.maxY)).reduce((x, y) => (math.min(x._1, y._1), math.min(x._2, y._2), math.max(x._3, y._3), math.max(x._4, y._4)))
      println("extentTuple4/2:" + extentTuple4)
      val extent: Extent = new Extent(extentTuple4._1, extentTuple4._2, extentTuple4._3, extentTuple4._4)
      val crs: CRS = t._2.crs


      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
      val stitchedTile: Raster[Tile] = Raster(tile, extent)

      val saveFilePath: String = "D:\\研究生材料\\Cube\\results\\" + productName + "_" + productType + "_" + bandName + "_" + bandPlatform + "_" + timestampSeconds.toString + ".tif"
      GeoTiff(stitchedTile, crs).write(saveFilePath)
    })
  }

  def addStyles(cubeRDD: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], visualizationParam: VisualizationParam): Array[(RDD[(CubeTileKey, MultibandTile)], TileLayerMetadata[SpatialKey])] = {
    var multibandTileRdd: Array[(RDD[(CubeTileKey, MultibandTile)], TileLayerMetadata[SpatialKey])] = cubeRDD.map(rdd => (rdd._1.map(t => {
      val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
      multiBandTiles.append(t._2)
      (t._1, MultibandTile(multiBandTiles))
    }), rdd._2))


    multibandTileRdd.map(rdd => {
      var stylingRDD: RDD[(CubeTileKey, MultibandTile)] = rdd._1
      val minNum: Int = visualizationParam.getMin.length
      val maxNum: Int = visualizationParam.getMax.length

      if (minNum * maxNum != 0) {
        val minMaxBand: (Double, Double) = stylingRDD.map(t => {
          val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN).filter(x => x != Int.MinValue)
          if (noNaNArray.nonEmpty) {
            (noNaNArray.min, noNaNArray.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
        val minVis: Double = visualizationParam.getMin.headOption.getOrElse(0.0)
        val maxVis: Double = visualizationParam.getMax.headOption.getOrElse(1.0)
        val gainBand: Double = (maxVis - minVis) / (minMaxBand._2 - minMaxBand._1)
        val biasBand: Double = (minMaxBand._2 * minVis - minMaxBand._1 * maxVis) / (minMaxBand._2 - minMaxBand._1)
        stylingRDD = stylingRDD.map(t => (t._1, t._2.mapBands((_, tile) => {
          Add(Multiply(tile, gainBand), biasBand)
        })))
      }

      if (visualizationParam.getPalette.nonEmpty) {
        val paletteVis: List[String] = visualizationParam.getPalette
        val colorVis: List[Color] = paletteVis.map(t => {
          try {
            val color: Color = Color.valueOf(t)
            color
          } catch {
            case e: Exception =>
              throw new IllegalArgumentException(s"Wrong Input Color: $t")
          }
        })
        val colorRGB: List[(Double, Double, Double)] = colorVis.map(t => (t.getRed, t.getGreen, t.getBlue))
        val minMaxBand: (Double, Double) = stylingRDD.map(t => {
          val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN).filter(x => x != Int.MinValue)
          if (noNaNArray.nonEmpty) {
            (noNaNArray.min, noNaNArray.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))

        val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
        stylingRDD = stylingRDD.map(t => {
          val len: Int = colorRGB.length - 1
          val bandR: Tile = t._2.band(0).mapDouble(d => {
            var R: Double = 0.0
            for (i <- 0 to len) {
              if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                R = colorRGB(i)._1 * 255.0
              }
            }
            R
          })
          val bandG: Tile = t._2.band(0).mapDouble(d => {
            var G: Double = 0.0
            for (i <- 0 to len) {
              if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                G = colorRGB(i)._2 * 255.0
              }
            }
            G
          })
          val bandB: Tile = t._2.band(0).mapDouble(d => {
            var B: Double = 0.0
            for (i <- 0 to len) {
              if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                B = colorRGB(i)._3 * 255.0
              }
            }
            B
          })
          (t._1, MultibandTile(bandR, bandG, bandB))
        })
      }
      (stylingRDD, rdd._2)
    })
  }


  def visualizeOnTheFly(implicit sc: SparkContext, cubeRDD: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], visualizationParam: VisualizationParam): Unit = {
    val bands: Array[String] = visualizationParam.getBands.toArray
    val tol_urljson: ArrayBuffer[JSONObject] = ArrayBuffer()

    for (i <- bands.indices) {
      cubeRDD.foreach(rdd => {
        val selected = rdd._1.filter(t => t._1.bandKey.bandName.equals(bands(i)))
        (selected, rdd._2)
      })
      val styledRDD: Array[(RDD[(CubeTileKey, MultibandTile)], TileLayerMetadata[SpatialKey])] = addStyles(cubeRDD, visualizationParam)
      styledRDD.foreach(rdd => {
        val cubeRDDwithMutiBand: RDD[(SpatialKey, MultibandTile)] = rdd._1.map(t => {
          (new SpatialKey(t._1.spaceKey.col, t._1.spaceKey.row), t._2)
        })

        var cubeTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(cubeRDDwithMutiBand, rdd._2)

        if (COGUtil.tileDifference > 0) {
          val levelUp: Int = COGUtil.tileDifference
          val layoutOrigin: LayoutDefinition = cubeTMS.metadata.layout
          val extentOrigin: Extent = cubeTMS.metadata.layout.extent
          val extentIntersect: Extent = extentOrigin.intersection(COGUtil.extent).orNull
          val layoutCols: Int = math.max(math.ceil((extentIntersect.xmax - extentIntersect.xmin) / 256.0 / layoutOrigin.cellSize.width * (1 << levelUp)).toInt, 1)
          val layoutRows: Int = math.max(math.ceil((extentIntersect.ymax - extentIntersect.ymin) / 256.0 / layoutOrigin.cellSize.height * (1 << levelUp)).toInt, 1)
          val extentNew: Extent = Extent(extentIntersect.xmin, extentIntersect.ymin, extentIntersect.xmin + layoutCols * 256.0 * layoutOrigin.cellSize.width / (1 << levelUp), extentIntersect.ymin + layoutRows * 256.0 * layoutOrigin.cellSize.height / (1 << levelUp))

          val tileLayout: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
          val layoutNew: LayoutDefinition = LayoutDefinition(extentNew, tileLayout)
          cubeTMS = cubeTMS.reproject(cubeTMS.metadata.crs, layoutNew)._2
        }

        val tmsCrs: CRS = CRS.fromEpsgCode(3857)
        val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
        val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
          cubeTMS.reproject(tmsCrs, layoutScheme)
        }

        val outputPath: String = GlobalConfig.Others.onTheFlyStorage
        // Create the attributes store that will tell us information about our catalog.
        val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
        // Create the writer that we will use to store the tiles in the local catalog.
        val writer: FileLayerWriter = FileLayerWriter(attributeStore)

        if (zoom < Trigger.level) {
          throw new InternalError("内部错误，切分瓦片层级没有前端TMS层级高")
        }

        Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
          if (Trigger.level - z <= 2 && Trigger.level - z >= 0) {
            val layerId: LayerId = LayerId(Trigger.dagId, z)
            println(layerId)
            // If the layer exists already, delete it out before writing
            if (attributeStore.layerExists(layerId)) {
              //        new FileLayerManager(attributeStore).delete(layerId)
              try {
                writer.overwrite(layerId, rdd)
              } catch {
                case e: Exception =>
                  e.printStackTrace()
              }
            }
            else {
              writer.write(layerId, rdd, ZCurveKeyIndexMethod)
            }
          }
        }

        if (sc.master.contains("local")) {
          CoverageUtils.makeTIFF(reprojected, "cube" + rdd._1.collect().head._1.bandKey.bandName)
        }
      })

      val rasterJsonObject: JSONObject = new JSONObject
      if (visualizationParam.getFormat == "png") {
        rasterJsonObject.put(Trigger.layerName, GlobalConfig.Others.tmsPath + Trigger.dagId + "/{z}/{x}/{y}.png")
      }
      else {
        rasterJsonObject.put(Trigger.layerName, GlobalConfig.Others.tmsPath + Trigger.dagId + "/{z}/{x}/{y}.jpg")
      }
      tol_urljson.append(rasterJsonObject)
    }

    val jsonObject: JSONObject = new JSONObject
    val dimObject: JSONObject = new JSONObject
    val dimension: ArrayBuffer[JSONObject] = ArrayBuffer()
    dimObject.put("name", "bands")
    dimObject.put("values", bands)
    dimension.append(dimObject)
    jsonObject.put("raster", tol_urljson.toArray)
    jsonObject.put("bands", bands)
    jsonObject.put("dimension", dimension.toArray)

    PostSender.shelvePost("cube", jsonObject)

  }

  def add(cube1: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
          cube2: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    cubeTemplate(cube1, cube2, (tile1, tile2) => Add(tile1, tile2))
  }

  def subtract(cube1: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
               cube2: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    cubeTemplate(cube1, cube2, (tile1, tile2) => Subtract(tile1, tile2))
  }

  def divide(cube1: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
             cube2: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    cubeTemplate(cube1, cube2, (tile1, tile2) => Divide(tile1, tile2))
  }

  def multiply(cube1: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
               cube2: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    cubeTemplate(cube1, cube2, (tile1, tile2) => Multiply(tile1, tile2))
  }

  def addNum(cube: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
             i: AnyVal): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    i match {
      case (x: Int) => cubeTemplate(cube, tile => Add(tile, x))
      case (x: Double) => cubeTemplate(cube, tile => Add(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  def subtractNum(cube: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
                  i: AnyVal): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    i match {
      case (x: Int) => cubeTemplate(cube, tile => Subtract(tile, x))
      case (x: Double) => cubeTemplate(cube, tile => Subtract(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  def divideNum(cube: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
                i: AnyVal): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    i match {
      case (x: Int) => cubeTemplate(cube, tile => Divide(tile, x))
      case (x: Double) => cubeTemplate(cube, tile => Divide(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  def multiplyNum(cube: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])],
                  i: AnyVal): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    i match {
      case (x: Int) => cubeTemplate(cube, tile => Multiply(tile, x))
      case (x: Double) => cubeTemplate(cube, tile => Multiply(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }
}