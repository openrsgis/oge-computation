package whu.edu.cn.entity

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.OGEDataType.OGEDataType
import whu.edu.cn.util.DateUtils

import java.time.{LocalDate, LocalDateTime}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import scala.collection.mutable
import scala.util.control.Breaks

class CoverageMetadata extends Serializable with LazyLogging {
  var coverageId: String = _
  var product: String = _
  var productType: String = _
  var productDescription: String = _
  var platformName: String = _
  var path: String = _
  var geom: Geometry = _
  var measurement: String = _
  var measurementRank: Int = _
  var time: LocalDateTime = _
  var crs: CRS = _
  var dataType: OGEDataType = _
  var resolution: Double = _

  def getCoverageID: String = {
    this.coverageId
  }

  def setCoverageID(coverageId: String): Unit = {
    this.coverageId = coverageId
  }

  def getProduct: String = {
    this.product
  }

  def setProduct(product: String): Unit = {
    this.product = product
  }

  def getProductType: String = {
    this.productType
  }

  def setProductType(productType: String): Unit = {
    this.productType = productType
  }

  def getProductDescription: String = {
    this.productDescription
  }

  def setProductDescription(productDescription: String): Unit = {
    this.productDescription = productDescription
  }

  def getPlatformName: String = {
    this.platformName
  }

  def setPlatformName(platformName: String): Unit = {
    this.platformName = platformName
  }

  def getPath: String = {
    this.path
  }

  def setPath(path: String): Unit = {
    this.path = path
  }

  def getGeom: Geometry = {
    this.geom
  }

  def setGeom(geomWkt: String): Unit = {
    this.geom = geotrellis.vector.io.readWktOrWkb(geomWkt)
  }

  def getMeasurement: String = {
    this.measurement
  }

  def setMeasurement(measurement: String): Unit = {
    this.measurement = measurement
  }

  def getMeasurementRank: Int = {
    this.measurementRank
  }

  def setMeasurementRank(measurementRank: Int): Unit = {
    this.measurementRank = measurementRank
  }

  def getTime: LocalDateTime = {
    this.time
  }

  def setTime(time: String): Unit = {
    DateUtils.parseDateTime(time) match {
      case Some(dt) =>
        this.time = dt
      case None =>
        logger.error("无法识别的日期格式,内容为:" + time)
    }
  }

  def getCrs: CRS = {
    this.crs
  }

  def setCrs(crs: CRS): Unit = {
    this.crs = crs
  }

  def getDataType: OGEDataType = {
    this.dataType
  }

  def setDataType(dataType: String): Unit = {
    this.dataType = OGEDataType.withName(dataType)
  }

  def getResolution: Double = {
    this.resolution
  }

  def setResolution(resolution: Double): Unit = {
    this.resolution = resolution
  }

  override def toString = s"CoverageMetadata(coverageId=$coverageId, product=$product, path=$path, geom=$geom, measurement=$measurement, measurementRank=$measurementRank, time=$time, crs=$crs, dataType=$dataType, resolution=$resolution)"
}
