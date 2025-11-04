package whu.edu.cn.entity

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable

/**
 * 所有允许的算子输入/输出数据类型
 */
sealed trait AllowedData

/**
 * 整数类型数据
 *
 * @param data 整型值
 */
case class IntData(data: Int) extends AllowedData

/**
 * 长整型数据
 *
 * @param data 长整型值
 */
case class LongData(data: Long) extends AllowedData

/**
 * 浮点型数据
 *
 * @param data 浮点型值
 */
case class DoubleData(data: Double) extends AllowedData

/**
 * 字符串类型数据
 *
 * @param data 字符串值
 */
case class StringData(data: String) extends AllowedData

/**
 * 栅格RDD类型数据
 *
 * @param metadata 瓦片图层的元数据
 * @param data     瓦片RDD数据内容，每个瓦片基于时空波段信息为键进行索引
 */
case class CoverageRDD(metadata: TileLayerMetadata[SpaceTimeKey], data: RDD[(SpaceTimeBandKey, MultibandTile)]) extends AllowedData

/**
 * 栅格集合RDD数据
 *
 * @param data 存放多个栅格瓦片的RDD数据类型
 */
case class CoverageCollectionRDD(data: mutable.Map[String, CoverageRDD]) extends AllowedData

/**
 * 矢量数据RDD类型
 *
 * @param data 矢量数据，为键值对元组，键是数据名称，值为一个包含矢量数据本身及其属性表的元组
 */
case class FeatureRDD(data: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends AllowedData