package whu.edu.cn.util

import geotrellis.vector.Extent
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.{CRS, GeodeticCalculator}
import org.locationtech.jts.geom._
import org.locationtech.jts.linearref.LengthLocationMap
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.mutable.ListBuffer

object GeometryUtil {
  private val geometryFactory :GeometryFactory= new GeometryFactory()

  /**
    * 计算多边形的测地线面积（单位：平方米）
    *
    * @param polygon JTS Polygon（必须使用WGS84坐标系）
    */
  @throws[Exception]
  def calculateGeodeticArea(polygon: Polygon): Double = {
    val crs = CRS.decode("EPSG:4326")
    // WGS84
    val calculator = new GeodeticCalculator(crs)
    // 将多边形三角剖分（简化版：实际需使用JTS的Triangulation）
    val coords = polygon.getCoordinates
    var totalArea = 0.0
    // 以多边形第一个点为基准，与其他点组成三角形
    val base = coords(0)
    var i = 1
    while ( {
      i < coords.length - 1
    }) {
      val p1 = coords(i)
      val p2 = coords(i + 1)
      totalArea += calculateTriangleArea(calculator, base, p1, p2)

      {
        i += 1; i - 1
      }
    }
    Math.abs(totalArea) // 确保面积为正

  }

  /**
    * 计算单个三角形的测地线面积（球面三角形面积公式）
    */
  @throws[Exception]
  private def calculateTriangleArea(calc: GeodeticCalculator, p0: Coordinate, p1: Coordinate, p2: Coordinate) = { // 计算三条边的测地线距离
    val a = calculateGeodeticDistance(calc, p1, p2)
    // 边a（p1-p2）
    val b = calculateGeodeticDistance(calc, p0, p2)
    // 边b（p0-p2）
    val c = calculateGeodeticDistance(calc, p0, p1)
    // 边c（p0-p1）
    // 使用球面三角形面积公式：S = R² * (A + B + C - π)
    val R = 6371008.8
    // 地球平均半径（米）
    val s = (a + b + c) / 2
    val area = 4 * Math.atan(Math.sqrt(Math.tan(s / 2) * Math.tan((s - a) / 2) * Math.tan((s - b) / 2) * Math.tan((s - c) / 2)))
    R * R * area
  }

  /**
    * 计算两点间的测地线距离（单位：米）
    */
  @throws[Exception]
  private def calculateGeodeticDistance(calc: GeodeticCalculator, p1: Coordinate, p2: Coordinate) = {
    calc.setStartingPosition(JTS.toDirectPosition(p1, calc.getCoordinateReferenceSystem))
    calc.setDestinationPosition(JTS.toDirectPosition(p2, calc.getCoordinateReferenceSystem))
    calc.getOrthodromicDistance
  }

  /**
    * 计算几何的测地线长度（单位：米）
    * @param geom     输入的几何（Point/LineString/LinearRing/Polygon/MultiPolygon）
    * @param geodesic 是否启用测地线计算（true=球面，false=平面）
    */
  def calculateGeodesicLength(geom: Geometry): Double = {
    val crs: CoordinateReferenceSystem = CRS.decode("EPSG:4326") // WGS84
    val calculator = new GeodeticCalculator(crs)
    geom match {
      case point: Point =>
        0.0 // 点的长度为0

      case multiPoint: MultiPoint => 0.0

      case line: LineString =>
        calculateLineStringDistance(line, calculator)

      case ring: LinearRing =>
        calculateLineStringDistance(ring, calculator) // LinearRing 是闭合的 LineString

      case polygon: Polygon =>
        val exteriorRing = polygon.getExteriorRing
        calculateLineStringDistance(exteriorRing, calculator) // 仅计算外环

      case multiLineString: MultiLineString =>
        (0 until multiLineString.getNumGeometries).map { i =>
          val lineString = multiLineString.getGeometryN(i).asInstanceOf[LineString]
          calculateLineStringDistance(lineString, calculator)
        }.sum

      case multiPolygon: MultiPolygon =>
        (0 until multiPolygon.getNumGeometries).map { i =>
          val polygon = multiPolygon.getGeometryN(i).asInstanceOf[Polygon]
          calculateLineStringDistance(polygon.getExteriorRing, calculator)
        }.sum

      case geometryCollection: GeometryCollection =>
        (0 until geometryCollection.getNumGeometries).map { i =>
          calculateGeodesicLength(geometryCollection.getGeometryN(i))
        }.sum

      case _ =>
        throw new IllegalArgumentException(s"不支持的类型: ${geom.getGeometryType}")
    }
  }

  def calculateGeodesicPerimeter(geom: Geometry): Double = {
    val crs: CoordinateReferenceSystem = CRS.decode("EPSG:4326") // WGS84
    val calculator = new GeodeticCalculator(crs)
    geom match {
      case point: Point =>
        0.0 // 点的长度为0

      case multiPoint: MultiPoint => 0.0

      case line: LineString => 0.0

      case ring: LinearRing => 0.0

      case polygon: Polygon =>
        val exteriorRing = polygon.getExteriorRing
        calculateLineStringDistance(exteriorRing, calculator) // 仅计算外环

      case multiLineString: MultiLineString => 0.0

      case multiPolygon: MultiPolygon =>
        (0 until multiPolygon.getNumGeometries).map { i =>
          val polygon = multiPolygon.getGeometryN(i).asInstanceOf[Polygon]
          calculateLineStringDistance(polygon.getExteriorRing, calculator)
        }.sum

      case geometryCollection: GeometryCollection =>
        (0 until geometryCollection.getNumGeometries).map { i =>
          calculateGeodesicPerimeter(geometryCollection.getGeometryN(i))
        }.sum

      case _ =>
        throw new IllegalArgumentException(s"不支持的类型: ${geom.getGeometryType}")
    }
  }

  /** 计算 LineString/LinearRing 的测地线长度 */
  private def calculateLineStringDistance(
                                           line: LineString,
                                           calculator: GeodeticCalculator
                                         ): Double = {
    val coords = line.getCoordinates
    (0 until coords.length - 1).map { i =>
      calculator.setStartingPosition(JTS.toDirectPosition(coords(i), calculator.getCoordinateReferenceSystem))
      calculator.setDestinationPosition(JTS.toDirectPosition(coords(i + 1), calculator.getCoordinateReferenceSystem))
      calculator.getOrthodromicDistance // 两点间的测地线距离
    }.sum
  }

//  def cutLines(geom: OGEGeometry, distances: List[Double], crs: String = "EPSG:4326"): OGEGeometry = {
//    val crsObj: CoordinateReferenceSystem = CRS.decode(crs)
//
//    val resultGeometry: Geometry = geom.geometry match {
//      case line: LineString =>
//        processLine(line, distances, geometryFactory)
//      case multiLine: MultiLineString =>
//        processMultiLine(multiLine, distances, geometryFactory)
//      case linearRing: LinearRing =>
//        val line = geometryFactory.createLineString(linearRing.getCoordinates)
//        processLine(line, distances, geometryFactory)
//      case _ =>
//        geometryFactory.createMultiLineString(Array.empty[LineString])
//    }
//
//    new OGEGeometry(resultGeometry, geom.geodesic)
//  }
//
//  private def processLine(line: LineString, distances: List[Double], factory: GeometryFactory): MultiLineString = {
//    var currentDistance = 0.0
//    val subLines = distances.flatMap { dist =>
//      if (currentDistance + dist <= line.getLength) {
//        val startPoint = line.interpolate(currentDistance)
//        val endPoint = line.interpolate(currentDistance + dist)
//        val subLine = factory.createLineString(Array(startPoint, endPoint))
//        currentDistance += dist
//        Some(subLine)
//      } else {
//        None
//      }
//    } ++ {
//      if (currentDistance < line.getLength) {
//        val startPoint = line.interpolate(currentDistance)
//        val endPoint = line.interpolate(line.getLength)
//        val subLine = factory.createLineString(Array(startPoint, endPoint))
//        List(subLine)
//      } else {
//        List.empty[LineString]
//      }
//    }
//
//    factory.createMultiLineString(subLines.toArray)
//  }
//
//  private def processMultiLine(multiLine: MultiLineString, distances: List[Double], factory: GeometryFactory): MultiLineString = {
//    val allSubLines = (0 until multiLine.getNumGeometries).flatMap { i =>
//      val line = multiLine.getGeometryN(i).asInstanceOf[LineString]
//      val subLines = processLine(line, distances, factory).geoms.toList
//      subLines
//    }
//
//    factory.createMultiLineString(allSubLines.toArray)
//  }




//  private def splitLine(line: LineString, distances: List[Double], factory: GeometryFactory): MultiLineString = {
//    var currentDistance = 0.0
//    val subLines = distances.flatMap { dist =>
//      if (currentDistance + dist <= line.getLength) {
//        val startPoint = line.getPointN(findClosestPointIndex(line, currentDistance))
//        val endPoint = line.getPointN(findClosestPointIndex(line, currentDistance + dist))
//        val subLine = factory.createLineString(Array(startPoint.getCoordinate, endPoint.getCoordinate))
//        currentDistance += dist
//        Some(subLine)
//      } else {
//        None
//      }
//    }
//    if (currentDistance < line.getLength) {
//      val startPoint = line.getPointN(findClosestPointIndex(line, currentDistance))
//      val endPoint = line.getEndPoint
//      val subLine = factory.createLineString(Array(startPoint.getCoordinate, endPoint.getCoordinate))
//      subLines :+ subLine
//    } else {
//      subLines
//    }
//
//    factory.createMultiLineString(subLines.toArray)
//  }

  /**
    * 切割单条LineString
    */
  def splitLine(line: LineString, distances: List[Double], geodesic: Boolean): MultiLineString = {
    val segments = ListBuffer[LineString]()
    var remainingLine = line
    var remainingDistances = distances

    while (remainingDistances.nonEmpty && !remainingLine.isEmpty) {
      val distance = remainingDistances.head
      if (distance <= 0) {
        remainingDistances = remainingDistances.tail
      } else {
        val splitResult = splitLineAtDistance(remainingLine, distance, geodesic)
        segments += splitResult._1
        remainingLine = splitResult._2
        remainingDistances = remainingDistances.tail
      }
    }

    if (!remainingLine.isEmpty) {
      segments += remainingLine
    }

    geometryFactory.createMultiLineString(segments.toArray)
  }
  /**
    * 在指定距离处切割LineString
    */
  private def splitLineAtDistance(line: LineString, distance: Double, geodesic: Boolean): (LineString, LineString) = {
    if (geodesic) {
      splitLineAtGeodesicDistance(line, distance)
    } else {
      splitLineAtPlanarDistance(line, distance)
    }
  }
  /**
    * 平面距离切割
    */
  private def splitLineAtPlanarDistance(line: LineString, distance: Double): (LineString, LineString) = {
    val totalLength = line.getLength
    val actualDistance = math.min(distance, totalLength)

    val fraction = actualDistance / totalLength
    val splitPoint = line.getPointN(line.getNumPoints - 1).getCoordinate // 默认取终点

    // 使用JTS的split方法
    val splitter = new LengthLocationMap(line)
    val loc = splitter.getLocation(actualDistance)
    val splitCoord = line.getCoordinateSequence.getCoordinate(loc.getSegmentIndex)

    // 创建两个子线段
    val coords1 = line.getCoordinates.take(loc.getSegmentIndex + 1) :+ splitCoord
    val coords2 = splitCoord +: line.getCoordinates.drop(loc.getSegmentIndex + 1)

    (geometryFactory.createLineString(coords1), geometryFactory.createLineString(coords2))
  }

  /**
    * 测地线距离切割
    */
  private def splitLineAtGeodesicDistance(line: LineString, distance: Double): (LineString, LineString) = {
    val crs = CRS.decode("EPSG:4326")
    val calculator = new GeodeticCalculator(crs)
    var accumulatedDistance = 0.0
    var splitIndex = 0

    // 找到切割点
    for (i <- 0 until line.getNumPoints - 1) {
      val p1 = line.getCoordinateN(i)
      val p2 = line.getCoordinateN(i + 1)

      calculator.setStartingPosition(JTS.toDirectPosition(p1, crs))
      calculator.setDestinationPosition(JTS.toDirectPosition(p2, crs))
      val segmentDistance = calculator.getOrthodromicDistance

      if (accumulatedDistance + segmentDistance >= distance) {
        // 计算切割点在此线段上的位置
        val ratio = (distance - accumulatedDistance) / segmentDistance
        val splitCoord = new Coordinate(
          p1.x + ratio * (p2.x - p1.x),
          p1.y + ratio * (p2.y - p1.y)
        )

        // 创建两个子线段
        val coords1 = line.getCoordinates.take(i + 1) :+ splitCoord
        val coords2 = splitCoord +: line.getCoordinates.drop(i + 1)

        return (geometryFactory.createLineString(coords1), geometryFactory.createLineString(coords2))
      }

      accumulatedDistance += segmentDistance
      splitIndex = i + 1
    }

    // 如果距离超过总长度，返回完整线段和空线段
    (line, geometryFactory.createLineString())
  }

  /**
    * 切割MultiLineString
    */
  def splitMultiLine(multiLine: MultiLineString, distances: List[Double], geodesic: Boolean): MultiLineString = {
    val allSegments = ListBuffer[LineString]()
    var remainingDistances = distances

    for (i <- 0 until multiLine.getNumGeometries) {
      val line = multiLine.getGeometryN(i).asInstanceOf[LineString]
      val splitResult = splitLine(line, remainingDistances, geodesic)

      // 添加所有子线段
      for (j <- 0 until splitResult.getNumGeometries) {
        allSegments += splitResult.getGeometryN(j).asInstanceOf[LineString]
      }

      // 更新剩余距离
      val usedDistance = splitResult.getGeometryN(0).asInstanceOf[LineString].getLength
      remainingDistances = remainingDistances.map(_ - usedDistance).filter(_ > 0)
    }

    geometryFactory.createMultiLineString(allSegments.toArray)
  }

  def getExtentByString(extent: String): Extent = {
    val extentArray: Array[String] = extent.replace("[", "").replace("]", "").split(",")
    new Extent(extentArray.head.toDouble, extentArray(1).toDouble, extentArray(2).toDouble, extentArray(3).toDouble)
  }

//  private def splitMultiLine(multiLine: MultiLineString, distances: List[Double], factory: GeometryFactory): MultiLineString = {
//    val allSubLines: Array[LineString] = (0 until multiLine.getNumGeometries).flatMap(i => {
//      val line = multiLine.getGeometryN(i).asInstanceOf[LineString]
//      val multi = splitLine(line, distances, factory)
//      (0 until multiLine.getNumGeometries).map(i => {
//        multi.getGeometryN(i).asInstanceOf[LineString]
//      }).toList
//    }).toArray
//    factory.createMultiLineString(allSubLines)
//  }
//
//  private def findClosestPointIndex(line: LineString, distance: Double): Int = {
//    var minDistance = Double.MaxValue
//    var closestIndex = 0
//    for (i <- 0 until line.getNumPoints) {
//      val currentPoint = line.getPointN(i)
//      val currentDistance = line.project(currentPoint.getCoordinate)
//      val diff = math.abs(currentDistance - distance)
//      if (diff < minDistance) {
//        minDistance = diff
//        closestIndex = i
//      }
//    }
//    closestIndex
//  }
}
