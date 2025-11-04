package whu.edu.cn.util

import geotrellis.layer.{Bounds, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.spark.store.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.{CoverageRDD, FeatureRDD, SpaceTimeBandKey}

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 用于测试场景（例如插件算子测试、计算端测试）的实用类
 */
object TestUtils {

  /**
   * 创建一个用于本地测试的SparkContext上下文对象
   *
   * @return Spark上下文对象，使用本地CPU计算
   */
  def createSparkContext(): SparkContext = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Test Spark Context")
    new SparkContext(config)
  }

  /**
   * 从本地文件系统加载TIFF文件为栅格RDD数据集
   *
   * @param context  Spark上下文
   * @param tiffPath TIFF文件路径
   * @return 加载后的栅格RDD数据集
   */
  def loadCoverageRDDFromTIFF(context: SparkContext, tiffPath: String): CoverageRDD = {
    // 从本地读取TIFF文件
    val inputRdd = context.hadoopMultibandGeoTiffRDD(s"file://$tiffPath")
    // 构建256 x 256瓦片布局
    val layoutScheme = FloatingLayoutScheme(256)
    // 收集读取瓦片元数据
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) = inputRdd.collectMetadata[SpatialKey](layoutScheme)
    // 像元数据类型
    val cellType = metadata.cellType
    // 瓦片布局信息
    val srcLayout = metadata.layout
    // 空间范围信息
    val srcExtent = metadata.extent
    // 坐标系信息
    val srcCrs = metadata.crs
    //  数据的键值范围，即SpatialKey的最小值和最大值
    val srcBounds = metadata.bounds
    // 以当前时间，构建时空索引键
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = dateFormat.format(new Date())
    // 转换为时间戳
    val dateInstant = dateFormat.parse(now).getTime
    // 加入时间创建瓦片元数据
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey._1, srcBounds.get.minKey._2, dateInstant), SpaceTimeKey(srcBounds.get.maxKey._1, srcBounds.get.maxKey._2, dateInstant))
    val metaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    // 根据上述瓦片布局与元数据，将整个栅格划分瓦片
    val tiled = inputRdd.tileToLayout[SpatialKey](metadata).cache()
    // 统计波段数量并命名为B1、B2的形式
    val bandCount: Int = tiled.first()._2.bandCount
    val measurementName = ListBuffer.empty[String]
    for (i <- 1 to bandCount) {
      measurementName.append(s"B$i")
    }
    // 遍历瓦片，为每个瓦片创建SpaceTimeBandKey索引（时间、空间和波段索引）
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, dateInstant), measurementName), t._2)
    })
    // 返回数据
    CoverageRDD(metaData, tiledOut)
  }

  /**
   * 加载GeoJSON文件为FeatureRDD类型
   *
   * @param context  Spark上下文
   * @param jsonPath GeoJSON文件，需要是FeatureCollection类型
   * @return 矢量要素RDD序列
   */
  def loadFeatureRDDFromGeoJson(context: SparkContext, jsonPath: String): FeatureRDD = {
    // 读取JSON文件内容
    val fileStream = new FileInputStream(jsonPath)
    // 读取 FeatureCollection
    val featureJSON = new FeatureJSON()
    val featureCollection = featureJSON.readFeatureCollection(fileStream)
    // 遍历Feature
    val pointer = featureCollection.features()
    val list = ListBuffer.empty[Geometry]
    while (pointer.hasNext) {
      val geometry = pointer.next().getDefaultGeometryProperty.getValue.asInstanceOf[Geometry]
      list.append(geometry)
    }
    // 转换为FeatureRDD
    // 暂时不考虑属性表
    var id = 0
    FeatureRDD(
      context.parallelize(list.map(item => {
        id += 1
        (id.toString, (item, mutable.Map.empty[String, Any]))
      }))
    )
  }

}