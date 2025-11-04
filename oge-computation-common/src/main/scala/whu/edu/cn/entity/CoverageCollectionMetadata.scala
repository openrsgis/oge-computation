package whu.edu.cn.entity

import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import whu.edu.cn.util.{DateUtils, GeometryUtil}

import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CoverageCollectionMetadata extends Serializable {
  var productName: String = _
  var sensorName: String = _
  var measurementName: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
  var startTime: LocalDateTime = _
  var endTime: LocalDateTime = _
  var extent: Extent = _
  var crs: CRS = _
  var cloudCoverMin: Float = 0
  var cloudCoverMax: Float = 100
  var customFiles: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
  var bands: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
  var coverageIDList: ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]


  def getCoverageIDList: mutable.ArrayBuffer[String] = {
    this.coverageIDList
  }


  def setCoverageIDList(value: String) = {
    val coverageIDList: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    if (value != null && value.nonEmpty) {
      if (value.head == '[' && value.last == ']') {
        coverageIDList ++= value.replace("[", "").replace("]", "").split(",")
      }
      else {
        coverageIDList.append(value)
      }
    }
    this.coverageIDList = coverageIDList
  }


  def getCustomFiles: mutable.ArrayBuffer[String] = {
    this.customFiles
  }

  def setCustomFiles(customFiles: String): Unit = {
    val customFile: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    if (customFiles != null && customFiles.nonEmpty) {
      if (customFiles.head == '[' && customFiles.last == ']') {
        customFile ++= customFiles.replace("[", "").replace("]", "").split(",")
      } else {
        customFile.append(customFiles)
      }
    }
    this.customFiles = customFile
  }

  def getBands: mutable.ArrayBuffer[String] = {
    this.bands
  }

  def setBands(bands: String): Unit = {
    val band: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    if (bands != null && bands.nonEmpty) {
      if (bands.head == '[' && bands.last == ']') {
        band ++= bands.replace("[", "").replace("]", "").split(",")
      }
      else {
        band.append(bands)
      }
    }
    this.bands = band
  }


  def setCloudCoverMin(cloudLevel: Float): Unit ={
    cloudCoverMin = cloudLevel
  }

  def setCloudCoverMax(cloudLevel: Float): Unit = {
    cloudCoverMax = cloudLevel
  }

  def getCloudCoverMin(): Float = {
    cloudCoverMin
  }

  def getCloudCoverMax(): Float ={
    cloudCoverMax
  }

  def getProductName: String = {
    this.productName
  }

  def setProductName(productName: String): Unit = {
    this.productName = productName
  }

  def getSensorName: String = {
    this.sensorName
  }

  def setSensorName(sensorName: String): Unit = {
    this.sensorName = sensorName
  }

  def getMeasurementName: mutable.ArrayBuffer[String] = {
    this.measurementName
  }

  def setMeasurementName(measurementName: String): Unit = {
    val measurement: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    if (measurementName.head == '[' && measurementName.last == ']') {
      measurement ++= measurementName.replace("[", "").replace("]", "").split(",")
    }
    else {
      measurement.append(measurementName)
    }
    if (this.measurementName.nonEmpty) {
      this.measurementName.clear()
    }
    this.measurementName = measurement
  }

  def getStartTime: LocalDateTime = {
    this.startTime
  }

  def setStartTime(startTime: String): Unit = {
    this.startTime = DateUtils.parseDateTime(startTime).get

  }

  def getEndTime: LocalDateTime = {
    this.endTime
  }

  def setEndTime(endTime: String): Unit = {
    this.endTime = DateUtils.parseDateTime(endTime).get
  }

  def getExtent: Extent = {
    this.extent
  }

  def setExtent(extent: String): Unit = {
    this.extent = GeometryUtil.getExtentByString(extent)
  }

  def getCrs: CRS = {
    this.crs
  }

  def setCrs(crs: String): Unit = {
    val crsDefinition: CRS = CRS.fromName(crs)
    this.crs = crsDefinition
  }

}
