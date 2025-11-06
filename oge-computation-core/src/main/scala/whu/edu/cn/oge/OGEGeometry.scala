package whu.edu.cn.oge

import org.apache.spark.SparkContext

import java.io.{StringReader, StringWriter}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.geotools.geojson.geom.GeometryJSON
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.GeodeticCalculator
import org.locationtech.jts.precision.GeometryPrecisionReducer
import org.locationtech.jts.dissolve.LineDissolver
import org.locationtech.jts.simplify.{DouglasPeuckerSimplifier, PolygonHullSimplifier, TopologyPreservingSimplifier}
import org.locationtech.jts.densify.Densifier
import org.geotools.referencing.CRS
import org.geotools.measure.Measure
import whu.edu.cn.util.GeometryUtil.{calculateGeodesicLength, calculateGeodesicPerimeter, calculateGeodeticArea, splitLine, splitMultiLine}

import scala.collection.mutable
//import org.json.JSONObject
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.locationtech.jts.operation.distance.DistanceOp
import org.opengis.referencing.crs.CoordinateReferenceSystem
import whu.edu.cn.util.GeometryUtil
import whu.edu.cn.entity.OGEGeometryType.{OGEGeometryType, stringToOGEGeometryType}
import whu.edu.cn.entity.OGEGeometryType

import scala.collection.mutable.ListBuffer

class OGEGeometry(val geometry: Geometry = null, var geodesic: Boolean = true) {
  val type_ : String = if (geometry != null) geometry.getGeometryType else "NullGeometry"
  def geometryToWKT(): String = {
    val writer = new WKTWriter()
    writer.write(this.geometry)
  }
  def geometryToWKB(): Array[Byte] = {
    val writer = new WKBWriter()
    writer.write(this.geometry)
  }
}
object OGEGeometry {
  //TODO:关于几何对象的初始投影是否需要讨论一下？构建的几何对象默认是WGS84坐标系？
  private val geometryFactory :GeometryFactory= new GeometryFactory()
  private val POINT="Point"
  private val LINESTRING="LineString"
  private val LINEARRING="LinearRing"
  private val POLYGON="Polygon"
  private val MULTI_POINT="MultiPoint"
  private val MULTI_LINESTRING="MultiLineString"
  private val MULTI_POLYGON="MultiPolygon"
  private val GEOMETRY_COLLECTION = "GeometryCollection"

  def wktToGeometry(wkt: String): Geometry = {
    val reader = new WKTReader()
    reader.read(wkt)
  }
  def wkbToGeometry(wkb: Array[Byte]): Geometry = {
    val reader = new WKBReader()
    reader.read(wkb)
  }

  def crsToSRID(crs: String): Int = {
    val crsCode = crs.split(":")(1).trim().toInt
    crsCode
  }

  /**
    *get Coordinate list from string
    *
    * @param coors original string. eg:[[ -109.05, 41], [-109.05, 37], [-102.05, 37], [-102.05, 41]]
    * @return eg:List(-109.05, 41, -109.05, 37, -102.05, 37,-102.05, 41)
    */
  def getCoorFromStr(coors:String):List[Coordinate]={
    val lonlat=coors.replace("[","").replace("]","").split(",")
    var coorList:List[Coordinate]=List.empty
    for(i <- 0 until lonlat.length by 2){
      val coor=new Coordinate(lonlat(i).toDouble,lonlat(i+1).toDouble)
      coorList=coorList:+coor
    }
    coorList
  }

  /**
    * acoording to coordinates and geomType ,build the correct geojson
    * and then, get Geometry from geojson
    *
    * @param coors the coordinate    这里的coors传的是gjson？
    * @param geomType  Geometry Type
    * @param isGeoJson if true ,coors is a geojson.if false coors is a array of coordinates
    *             * @return
    */
  def getGeomFromCoors(coors:String,geomType:String,isGeoJson:Boolean=false):Geometry={
    val jsonStr="{\"geometry\":{\"type\":\""+geomType+"\",\"coordinates\":"+coors+"}}"
    val gjson:GeometryJSON=new GeometryJSON(6)
    var reader:StringReader=null
    if(!isGeoJson){
      reader=new StringReader(jsonStr)
    }
    else{
      reader=new StringReader(coors)
    }
    var geom:Geometry=null
    try{
      geom=gjson.read(reader)
    }catch{
      case ex:Exception=>{ex.printStackTrace()}
    }
    geom
  }

  def load(geometryType: String, coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    geometryType match {
      case POINT => loadPoint(coords, crs)
      case LINESTRING => loadLineString(coords, crs, geodesic)
      case LINEARRING => loadLinearRing(coords, crs, geodesic)
      case POLYGON => loadPolygon(coords, crs, geodesic)
      case MULTI_POINT => loadMultiPoint(coords, crs)
      case MULTI_LINESTRING => loadMultiLineString(coords, crs, geodesic)
      case MULTI_POLYGON => loadMultiPolygon(coords, crs, geodesic)
      case _ => throw new IllegalArgumentException(s"Geometry type ${geometryType} not supported.")
    }
  }

  def loadPoint(coords: List[Any], crs: String = "EPSG:4326"): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createPoint()
      else{
        coords.head match {
          case _: Number => point(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      }
    new OGEGeometry(geometry)
  }

  //TODO mcr  maxError: Double = null先去掉这个参数
  def loadLineString(coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createLineString()
      else{
        coords.head match {
          case _: Number => lineString(toPythonString(coords), crs) //两两一组坐标
          case _: OGEGeometry => {
            val coordArray: Array[Coordinate] = coords.map(g => {
              val geom: Geometry = g.asInstanceOf[OGEGeometry].geometry
              if (geom.getGeometryType == "Point") geom.asInstanceOf[Point].getCoordinate
              else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
            }).toArray
            geometryFactory.createLineString(coordArray)
          }//必须是Point类型
          case _: List[Any] => lineString(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      }
    new OGEGeometry(geometry, geodesic)
  }
  def loadLinearRing(coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createLinearRing()
      else{
        coords.head match {
          case _: Number => linearRing(toPythonString(coords), crs) //两两一组坐标
          case _: OGEGeometry => {
            if (coords.length==1){
              val geom: Geometry = coords.head.asInstanceOf[OGEGeometry].geometry
              if (geom.getGeometryType == "LinearString") geometryFactory.createLinearRing(List.concat(geom.asInstanceOf[LineString].getCoordinates, geom.asInstanceOf[LineString].getStartPoint.getCoordinates).toArray)
              else if (geom.getGeometryType == "LinearRing") geom.asInstanceOf[LinearRing]
              else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
            }
            else{
              val coordArray: Array[Coordinate] = coords.map(g => {
                val geom: Geometry = g.asInstanceOf[OGEGeometry].geometry
                if (geom.getGeometryType == "Point") geom.asInstanceOf[Point].getCoordinate
                else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
              }).toArray
              geometryFactory.createLinearRing(coordArray)
            }
          }//可以是Point或LineString(首尾相连)
          case _: List[Any] => linearRing(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      }
    new OGEGeometry(geometry, geodesic)
  }

  def loadPolygon(coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    val geometry: Polygon =
      if (coords==null) geometryFactory.createPolygon()
      else{
        coords.head match {
          case _: Number => polygon(toPythonString(coords), crs) //两两一组坐标
          case g: OGEGeometry => {
            val geom: Geometry = g.asInstanceOf[OGEGeometry].geometry
            if (geom.getGeometryType == "LinearRing") geometryFactory.createPolygon(geom.asInstanceOf[LinearRing])
            else if (geom.getGeometryType == "Polygon") geom.asInstanceOf[Polygon]
            else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
            //TODO mcr 暂不考虑多边形孔洞的情况
          }//可以是Point或LineString或LineRing
          case _: List[Any] => polygon(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      } //TODO mcr 暂不帮助将收尾节点进行闭合
    new OGEGeometry(geometry, geodesic)
  }
  def loadMultiPoint(coords: List[Any], crs: String = "EPSG:4326"): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createMultiPoint()
      else{
        coords.head match {
          case _: Number => {
            val geom=geometryFactory.createMultiPointFromCoords(getCoorFromStr(toPythonString(coords)).toArray)
            geom.setSRID(crsToSRID(crs))
            geom
          }//两两一组坐标
          case _: OGEGeometry => {
            val pointArray: Array[Point] = coords.map(g => {
              val geom: Geometry = g.asInstanceOf[OGEGeometry].geometry
              if (geom.getGeometryType == "Point") geom.asInstanceOf[Point]
              else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
            }).toArray
            geometryFactory.createMultiPoint(pointArray)
          } //可以是Point
          case _: List[Any] => multiPoint(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      }
    new OGEGeometry(geometry)
  }
  def loadMultiLineString(coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createMultiLineString()
      else{
        coords.head match {
          case _: Number => {
            val geom=geometryFactory.createMultiLineString(Array(geometryFactory.createLineString(getCoorFromStr(toPythonString(coords)).toArray)))
            geom.setSRID(crsToSRID(crs))
            geom
          }//两两一组坐标，认为只包含单个LineString
          case _: OGEGeometry => {
            val lineStringArray: Array[LineString] = coords.map(g => {
              val geom: Geometry = g.asInstanceOf[OGEGeometry].geometry
              if (geom.getGeometryType == "LineString") geom.asInstanceOf[LineString]
              else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
            }).toArray
            geometryFactory.createMultiLineString(lineStringArray)
          }//可以是LineString
          case _: List[Any] => multiLineString(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      }
    new OGEGeometry(geometry, geodesic)
  }
  def loadMultiPolygon(coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createMultiPolygon()
      else{
        coords.head match {
          case _: Number => {
            val geom=geometryFactory.createMultiPolygon(Array(geometryFactory.createPolygon(getCoorFromStr(toPythonString(coords)).toArray)))
            geom.setSRID(crsToSRID(crs))
            geom
          }//两两一组坐标，认为只包含单个Polygon
          case _: OGEGeometry => {
            val polygonArray: Array[Polygon] = coords.map(g => {
              val geom: Geometry = g.asInstanceOf[OGEGeometry].geometry
              if (geom.getGeometryType == "Polygon") geom.asInstanceOf[Polygon]
              else if (geom.getGeometryType == "LinearRing") geometryFactory.createPolygon(geom.asInstanceOf[LinearRing])
              else throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
            }).toArray
            geometryFactory.createMultiPolygon(polygonArray)
          }//可以是Polygon或LinearRing
          case _: List[Any] => multiPolygon(toPythonString(coords), crs)
          case _ => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
        }
      }
    new OGEGeometry(geometry, geodesic) //TODO mcr scala中这样创建新变量可以吗，是否要加new
  }
  def loadGeometryCollection(coords: List[Any], crs: String = "EPSG:4326", geodesic: Boolean = true): OGEGeometry = {
    val geometry =
      if (coords==null) geometryFactory.createGeometryCollection()
      else {
        val geometryArray: Array[Geometry] = coords.map(g =>
          try {
            g.asInstanceOf[OGEGeometry].geometry
          }
          catch {
            case _: Throwable => throw new IllegalArgumentException(s"输入参数coords: ${coords} 类型不支持")
          }
        ).toArray
        geometryFactory.createGeometryCollection(geometryArray)
      }
    new OGEGeometry(geometry, geodesic)
  }

  /**
    * construct a point in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def point(coors:String,crs:String="EPSG:4326"):Point={
    val geom=geometryFactory.createPoint(getCoorFromStr(coors).head)
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct a LineString in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def lineString(coors:String,crs:String="EPSG:4326"):LineString={
    val geom=geometryFactory.createLineString(getCoorFromStr(coors).toArray)
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct a LinearRing in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def linearRing(coors:String,crs:String="EPSG:4326"):LinearRing={
    val geom=geometryFactory.createLinearRing(getCoorFromStr(coors).toArray)
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct a polygon in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def polygon(coors:String,crs:String="EPSG:4326"):Polygon={
    var coorsList = getCoorFromStr(coors)
    if (!coorsList.head.equals2D(coorsList.last)) coorsList = coorsList ::: List(coorsList.head)
    val geom=geometryFactory.createPolygon(coorsList.toArray) //如果折线不封闭，将其处理为封闭图形
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct a MultiPoint in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def multiPoint(coors:String,crs:String="EPSG:4326"):MultiPoint={
    val geom=getGeomFromCoors(coors,MULTI_POINT).asInstanceOf[MultiPoint]
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct a MultiLineString in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def multiLineString(coors:String,crs:String="EPSG:4326"):MultiLineString={
    val geom=getGeomFromCoors(coors,MULTI_LINESTRING).asInstanceOf[MultiLineString]
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct a MulitiPolygon in a crs
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param coors the coordinates.Comply with the coordinate format in geojson
    * @param crs the crs of the geometry
    * @return
    */
  def multiPolygon(coors:String,crs:String="EPSG:4326"):MultiPolygon={
    val geom=getGeomFromCoors(coors,MULTI_POLYGON).asInstanceOf[MultiPolygon]
    geom.setSRID(crsToSRID(crs))
    geom
  }

  /**
    * construct geometry collection from geomtery list
    *
    * @param geomteries the geometry array to construct geometry collection
    * @return
    */
  def geometryCollection(geomteries:List[Geometry]):GeometryCollection={
    val geom=geometryFactory.createGeometryCollection(geomteries.toArray)
    geom
  }

  /**
    * construct a geometry from geojson
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param gjson the geojson.
    * @param crs the crs of the geometry
    * @return
    */
  def geometry(gjson:com.alibaba.fastjson.JSONObject, crs:String="EPSG:4326"):OGEGeometry={
    val coors=gjson.getJSONObject("geometry").getJSONArray("coordinates")
    val geomtype=gjson.getJSONObject("geometry").get("type")
    val geom=getGeomFromCoors(coors.toString,geomtype.toString,isGeoJson = false)
    geom.setSRID(crsToSRID(crs))
    new OGEGeometry(geom)
  }


  /**
    * get the area of the geometry
    * default to compute the area of geometry in "EPSG:3857"
    *
    * @param geom the geometry to compute area
    * @param crs the projection
    * @return
    */
  def area(geom: OGEGeometry, crs: String="EPSG:3857"): Double = {
    if (geom.geodesic) { //计算测地线面积
      if (geom.type_ == "Polygon") calculateGeodeticArea(geom.geometry.asInstanceOf[Polygon])
      else if (geom.type_ == "MultiPolygon"){
        val areaList = (0 until geom.geometry.getNumGeometries)
          .map(i => calculateGeodeticArea(geom.geometry.getGeometryN(i).asInstanceOf[Polygon]))
          .toList
        areaList.sum
      }
      else if (geom.type_ == "GeometryCollection"){
        val areaList = (0 until geom.geometry.getNumGeometries)
          .map(i => {
            if (geom.geometry.getGeometryN(i).getGeometryType()=="Polygon")
              calculateGeodeticArea(geom.geometry.getGeometryN(i).asInstanceOf[Polygon])
            else 0
          })
          .toList
        areaList.sum
      }
      else 0.0
    }
    else { //计算面积
      val srcCrsCode="EPSG:"+ geom.geometry.getSRID
      if(srcCrsCode.equals(crs)){
        geom.geometry.getArea
      }
      else{
        reproject(geom,crs).geometry.getArea
      }
    }
  }


  /**
    * get the length of the geometry
    * default to compute the length of geometry in "EPSG:3857"
    *
    * @param geom the geometry to compute length
    * @param crs the projection
    * @return
    */
  //与GEE有区别，这里多边形的长度是指周长
  def length(geom:OGEGeometry,crs:String="EPSG:3857"):Double={
    if(geom.geodesic){
      calculateGeodesicLength(geom.geometry)
    }
    else{
      val srcCrsCode="EPSG:"+geom.geometry.getSRID
      if(srcCrsCode.equals(crs)){
        geom.geometry.getLength
      }
      else{
        reproject(geom,crs).geometry.getLength
      }
    }
  }

  def perimeter(geom:OGEGeometry,crs:String="EPSG:3857"):Double={
    if(geom.geodesic){
      calculateGeodesicPerimeter(geom.geometry)
    }
    else{
      val srcCrsCode="EPSG:"+geom.geometry.getSRID
      val newGeom = if(srcCrsCode.equals(crs)) geom else reproject(geom,crs)
      if (newGeom.type_ == "Polygon" || newGeom.type_ == "MultiPolygon") newGeom.geometry.getLength
      else if (newGeom.type_ == "GeometryCollection") {
        (0 until newGeom.geometry.getNumGeometries).map { i =>
          if (newGeom.geometry.getGeometryN(i).getGeometryType == "Polygon" || newGeom.geometry.getGeometryN(i).getGeometryType == "MultiPolygon") newGeom.geometry.getGeometryN(i).getLength else 0.0
        }.sum
      }
      else 0.0
    }
  }

  /**
    * Returns the bounding rectangle of the geometry.
    *
    * @param geom the geometry to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def bounds(geom:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val result = new OGEGeometry(geom.geometry.getEnvelope, geodesic = false) // 边界框始终为平面
    result.geometry.setSRID(geom.geometry.getSRID)
    reproject(result,crs)
  }


  def buffer(geom:OGEGeometry,distance:Double,crs:String=null):OGEGeometry={
    val result = {
      if(crs == null)
        new OGEGeometry(Geometry.buffer(geom.geometry, distance, "EPSG:"+geom.geometry.getSRID), geom.geodesic)
      else
        new OGEGeometry(Geometry.buffer(geom.geometry, distance, crs), geom.geodesic)
    }
    result
  }

  /**
    * Returns the centroid of geometry
    *
    * @param geom the geometry to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def centroid(geom:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val centroidGeom = if (geom.geodesic && (geom.type_ == "LineString" || geom.type_ == "LinearRing")) {
      // 测地线中心点：线段中点简化实现
      val coords = geom.geometry.getCoordinates
      val midIndex = coords.length / 2
      geometryFactory.createPoint(coords(midIndex))
    } else {
      geom.geometry.getCentroid // JTS平面中心点
    }
    val result = new OGEGeometry(centroidGeom, geom.geodesic)
    result.geometry.setSRID(geom.geometry.getSRID)
    reproject(result,crs)
  }

  /**
    * Returns the convex hull of the given geometry.
    *
    * @param geom the geometry to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def convexHull(geom:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val result = new OGEGeometry(geom.geometry.convexHull(), geom.geodesic)
    result.geometry.setSRID(geom.geometry.getSRID)
    reproject(result,crs)
  }

  /**
    * Computes the union of all the elements of this geometry.
    * only for GeometryCollection
    *
    * @param geom the geometry to operate
    * @param crs
    * @return
    */
  def dissolve(geom:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val result = new OGEGeometry(geom.geometry.union(), geom.geodesic)
    result.geometry.setSRID(geom.geometry.getSRID)
    reproject(result,crs)
  }

  /**
    * reproject the geometry to target crs.
    *
    *
    * @param geom the geometry to reproject
    * @param tarCrsCode the target crs
    * @return
    */
  def reproject(geom:OGEGeometry, tarCrsCode:String):OGEGeometry={

    val srcCrsCode="EPSG:"+geom.geometry.getSRID()
    if(srcCrsCode.equals(tarCrsCode)){
      geom
    }
    else{
      //Note:When processing coordinate transform, geotools defaults to x as latitude and y as longitude.it may cuase false transform
      //CRS.decode(srcCrsCode,true) can solve the problem
      val sourceCrs=CRS.decode(srcCrsCode,true)
      val targetCrs=CRS.decode(tarCrsCode,true)
      val transform=CRS.findMathTransform(sourceCrs,targetCrs)
      val transGeom=JTS.transform(geom.geometry,transform)
      val tarSRID=tarCrsCode.split(":")(1).toInt
      transGeom.setSRID(tarSRID)
      new OGEGeometry(transGeom, geom.geodesic)
    }
  }

  /**
    * Returns a GeoJSON string representation of the geometry.
    *
    * @param geom
    * @return
    */
  def toGeoJSONString(geom: Geometry):String={
    var gjsonStr:String=null
    val gjson:GeometryJSON=new GeometryJSON(6)
    val writer:StringWriter=new StringWriter()
    gjson.write(geom,writer)
    gjsonStr=writer.toString
    writer.close()
    gjsonStr
  }
  def toGeoJSONString(geom: OGEGeometry):String={ //TODO mcr 考虑是否在json中增加geodesics字段
    var gjsonStr:String=null
    val gjson:GeometryJSON=new GeometryJSON(6)
    val writer:StringWriter=new StringWriter()
    gjson.write(geom.geometry,writer)
    gjsonStr=writer.toString
    writer.close()
    gjsonStr
  }

  /**
    * Returns true iff one geometry contains the other.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def contains(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom1,crs).geometry.contains(reproject(geom2,crs).geometry)
  }

  /**
    * Returns true iff one geometry is contained in the other.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def containedIn(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom2,crs).geometry.contains(reproject(geom1,crs).geometry)
  }

  /**
    * Returns true iff the geometries are disjoint.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def disjoint(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom1,crs).geometry.disjoint(reproject(geom2,crs).geometry)
  }

  /**
    * Returns the minimum distance between two geometries.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:3857
    * @return
    */
  def distance(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:3857"):Double={
    reproject(geom1,crs).geometry.distance(reproject(geom2,crs).geometry)
    //TODO mcr 暂不考虑测地线距离
  }

  /**
    * Returns the result of subtracting the 'right' geometry from the 'left' geometry.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def difference(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val resGeom = reproject(geom1,crs).geometry.difference(reproject(geom2,crs).geometry)
    resGeom.setSRID(crsToSRID(crs))
    new OGEGeometry(resGeom, geom1.geodesic)
  }

  /**
    * Returns the intersection of the two geometries.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def intersection(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val resGeom=reproject(geom1,crs).geometry.intersection(reproject(geom2,crs).geometry)
    resGeom.setSRID(crsToSRID(crs))
    new OGEGeometry(resGeom, geom1.geodesic)
  }

  /**
    * Returns true iff the geometries intersect.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def intersects(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom1,crs).geometry.intersects(reproject(geom2,crs).geometry)
  }

  /**
    * Returns the symmetric difference between two geometries.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def symmetricDifference(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val resGeom=reproject(geom1,crs).geometry.symDifference(reproject(geom2,crs).geometry)
    resGeom.setSRID(crsToSRID(crs))
    new OGEGeometry(resGeom, geom1.geodesic)
  }

  /**
    * Returns the union of the two geometries.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def union(geom1:OGEGeometry,geom2:OGEGeometry,crs:String="EPSG:4326"):OGEGeometry={
    val resGeom=reproject(geom1,crs).geometry.union(reproject(geom2,crs).geometry)
    resGeom.setSRID(crsToSRID(crs))
    new OGEGeometry(resGeom, geom1.geodesic)
  }

  /**
    * Returns true if the geometries are within a specified distance.
    *
    * @param geom1 left geometry
    * @param geom2 right geometry
    * @param distance The distance threshold.
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:3857
    * @return
    */
  def withinDistance(geom1:OGEGeometry,geom2:OGEGeometry,distance:Double,crs:String="EPSG:3857"):Boolean={
    reproject(geom1,crs).geometry.isWithinDistance(reproject(geom2,crs).geometry,distance)
  }

  //Returns a GeoJSON-style list of the geometry's coordinates.
  def coordinates(geom:OGEGeometry): List[Any] = {
    val gjson:GeometryJSON=new GeometryJSON(6)
    val writer:StringWriter=new StringWriter()
    gjson.write(geom.geometry,writer)
    val escapedJson = writer.toString.replace("\\", "")//去除转义符
    val jsonobject: JSONObject = JSON.parseObject(escapedJson)
    writer.close()
    jsonobject.getJSONArray("coordinates").toArray().toList
  }

  def isUnbounded(geom:OGEGeometry): Boolean = {
    if (geom.geometry.getBoundaryDimension < 0) true else false
  }

  def geometryType(geom:OGEGeometry): String = geom.type_

  def geodesic(geom:OGEGeometry): Boolean = geom.geodesic

  def projection(geom:OGEGeometry): String = "EPSG:" + geom.geometry.getSRID

  def geometries(geom:OGEGeometry): List[OGEGeometry] = {
    (0 until geom.geometry.getNumGeometries)
      .map(i => new OGEGeometry(geom.geometry.getGeometryN(i), geom.geodesic))
      .toList
  }

  def cutLines(geom: OGEGeometry, distances: List[Double], crs: String = "EPSG:4326"): OGEGeometry = {
    val newGeom = reproject(geom, crs)
    val resultGeometry: Geometry = newGeom.geometry match {
      case line: LineString =>
        splitLine(line, distances, newGeom.geodesic)
      case multiLine: MultiLineString =>
        splitMultiLine(multiLine, distances, newGeom.geodesic)
      case linearRing: LinearRing =>
        val line = geometryFactory.createLineString(linearRing.getCoordinates)
        splitLine(line, distances, newGeom.geodesic)
      case _ =>
        geometryFactory.createMultiLineString()
    }
    new OGEGeometry(resultGeometry, newGeom.geodesic)
  }

  def simplify(geom: OGEGeometry, distanceTolerance: Double, crs: String="EPSG:4326", topoPreserved: Boolean = false): OGEGeometry = {
    val newGeom = reproject(geom, crs)
    val resGeom=
      if (topoPreserved) TopologyPreservingSimplifier.simplify(newGeom.geometry, distanceTolerance)
      else DouglasPeuckerSimplifier.simplify(newGeom.geometry, distanceTolerance)
    resGeom.setSRID(crsToSRID(crs))
    new OGEGeometry(resGeom, geom.geodesic)
  }

  def densify(geom: OGEGeometry, distanceTolerance: Double, crs: String="EPSG:4326"): OGEGeometry = {
    val newGeom = reproject(geom, crs)
    val resGeom= Densifier.densify(newGeom.geometry, distanceTolerance)
    resGeom.setSRID(crsToSRID(crs))
    new OGEGeometry(resGeom, geom.geodesic)
  }

  def closestPoint(geom1:OGEGeometry, geom2:OGEGeometry, terminateDistance: Double = 0.0): OGEGeometry = {
    val factory = geom1.geometry.getFactory
    if (geom1.geometry.isEmpty || geom2.geometry.isEmpty) {
      return null
    }
    if (isUnbounded(geom1) && isUnbounded(geom2)) {
      new OGEGeometry(factory.createPoint(new Coordinate(0, 0)))
    }
    else if (isUnbounded(geom1)) {
      new OGEGeometry(getArbitraryPoint(geom2.geometry))
    }
    else if (isUnbounded(geom2)) {
      new OGEGeometry(getArbitraryPoint(geom1.geometry))
    }
    val distanceOp: DistanceOp = new DistanceOp(geom1.geometry, geom2.geometry, terminateDistance)
    val closestPoints = distanceOp.nearestPoints()
    val nearestCoord = closestPoints(1)
    new OGEGeometry(geom1.geometry.getFactory.createPoint(nearestCoord), geom1.geodesic)
  }
  def closestPoints(geom1:OGEGeometry, geom2:OGEGeometry, terminateDistance: Double = 0.0): mutable.Map[String, OGEGeometry] = {
    val result = mutable.Map.empty[String, OGEGeometry]
    val factory = geom1.geometry.getFactory
    if (geom1.geometry.isEmpty || geom2.geometry.isEmpty) {
      return result
    }
    if (isUnbounded(geom1) && isUnbounded(geom2)) {
      result("geom1") = new OGEGeometry(factory.createPoint(new Coordinate(0, 0)))
      result("geom2") = new OGEGeometry(factory.createPoint(new Coordinate(0, 0)))
      return result
    }
    else if (isUnbounded(geom1)) {
      result("geom1") = new OGEGeometry(getArbitraryPoint(geom2.geometry))
      result("geom2") = new OGEGeometry(getArbitraryPoint(geom2.geometry))
      return result
    }
    else if (isUnbounded(geom2)) {
      result("geom1") = new OGEGeometry(getArbitraryPoint(geom1.geometry))
      result("geom2") = new OGEGeometry(getArbitraryPoint(geom1.geometry))
      return result
    }
    val distanceOp = new DistanceOp(geom1.geometry, geom2.geometry, terminateDistance)
    val closestPoints = distanceOp.nearestPoints()
    result("geom1") = new OGEGeometry(factory.createPoint(closestPoints(0)))
    result("geom2") = new OGEGeometry(factory.createPoint(closestPoints(1)))
    result
  }
  private def getArbitraryPoint(geom: Geometry): Point = {
    val coord = geom.getCoordinate
    geom.getFactory.createPoint(if(coord!=null) coord else new Coordinate(0, 0))
  }

  def visualize(implicit sc: SparkContext, geom: OGEGeometry, color:List[String], attribute:String): Unit = {
    val featureCollection = FeatureCollection.loadFromGeometry(sc, geom)
    FeatureCollection.visualize(featureCollection, color, attribute)
  }

  // 递归转换函数
  def toPythonString(list: Any): String = {
    var sb = new StringBuilder("[")
    list match {
      case nested: List[_] =>
        nested.foreach { elem =>
          sb.append(toPythonString(elem))
          sb.append(", ")
        }
        if (nested.nonEmpty) sb = sb.dropRight(2) // 移除最后的", "
      case num => return num.toString
    }
    sb.append("]")
    sb.toString
  }


  def main(args: Array[String]): Unit ={
//    import scala.util.parsing.json.JSON
//    val a = "[[[-109.05, 41], [-109.05, 37], [-102.05, 37], [-102.05, 41]]]"
//    print (JSON.parseFull(a) match {
//      case Some(data) =>
//        data.asInstanceOf[List[Any]]
//      case None =>
//        throw new IllegalArgumentException("Invalid nested list format")
//    })


//    var coors="[[[119.283461766823521,35.113845473433457], [119.285033114198498,35.11405167501087]],  [[119.186893667167482,34.88690637041627], [119.186947247282234,34.890273599368562]]]"
    //coors="[35.113845473433457,119.283461766823521]"
    //coors="[[[0,0],[0,20],[20,20],[20,0],[0,0]]]"
    val coors=List(List(List(108.09876, 37.200787),List(106.398901, 33.648651),List(114.972103, 33.340483),List(113.715685, 37.845557),List(108.09876, 37.200787)))
//    val coors_pythonString = toPythonString(coors)
    val geom=loadPolygon(coors)
//    val p=polygon("[[[0, 0], [40, 0], [40, 40], [0, 40], [0, 0]]]")
    val rp=reproject(geom,"EPSG:3857")
//    val jsonStr="{\n\"name\":\"网站\",\n\"num\":3,\n\"sites\":[ \"Google\", \"Runoob\", \"Taobao\" ]\n}"
//    val p1=point("[10,10]")
    print(rp.geometry.getArea)
    //    println("test")

  }

}
