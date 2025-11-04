package whu.edu.cn.entity

import org.locationtech.jts.geom

object OGEGeometryType extends Enumeration {
  type OGEGeometryType = Value

  val Point: Value = Value("Point")
  val LineString: Value = Value("LineString")
  val LinearRing: Value = Value("LinearRing")
  val Polygon: Value = Value("Polygon")
  val MultiPoint: Value = Value("MultiPoint")
  val MultiLineString: Value = Value("MultiLineString")
  val MultiPolygon: Value = Value("MultiPolygon")
  val GeometryCollection: Value = Value("GeometryCollection")

  def stringToOGEGeometryType(geometryTypeName: String): Option[OGEGeometryType] = {
    try {
      Some(OGEGeometryType.withName(geometryTypeName))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def whichGeometryType(geometry: geom.Geometry): OGEGeometryType = geometry match {
    case _: geom.Point => Point
    case _: geom.LineString => LineString
    case _: geom.LinearRing => LinearRing
    case _: geom.Polygon => Polygon
    case _: geom.MultiPoint => MultiPoint
    case _: geom.MultiLineString => MultiLineString
    case _: geom.MultiPolygon => MultiPolygon
    case _: geom.GeometryCollection => GeometryCollection
  }
}
