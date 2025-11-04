package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.locationtech.jts.geom._
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity.BatchParam
import whu.edu.cn.util.PostSender

import java.io.FileWriter
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.matching.Regex
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.{ClientUtil, HbaseServiceUtil, PostgresqlServiceUtil, PostgresqlUtilDev}

import scala.io.Source

class Feature(val geometry: OGEGeometry = null, val properties: mutable.Map[String, Any] = mutable.Map(), var id: String = "") { //可以不必是一个RDD结构
  val type_ : String  = "Feature"
}

object Feature  extends StrictLogging{

  //Used to read vectors in the database
  def loadFromGeometry(geometry: OGEGeometry, properties: mutable.Map[String, Any] = mutable.Map.empty[String, Any]): Feature = {
    new Feature(geometry, properties)
  }
  def loadFromGeojson(gjson: String, properties: mutable.Map[String, Any] = mutable.Map.empty[String, Any], crs: String = "EPSG:4326"): Feature = {
    val escapedJson = gjson.replace("\\", "")
    val jsonobject: JSONObject = JSON.parseObject(escapedJson)
    if (jsonobject.getString("type")=="FeatureCollection") {
      val feature: JSONObject = jsonobject.getJSONArray("features").getJSONObject(0) 
      val geom = if (feature.containsKey("geometry")) { 
        OGEGeometry.geometry(feature, crs)
      } else null
      val id = if (feature.containsKey("id")) feature.getString("id") else "" 
      val prop = if (feature.containsKey("properties")) { 
        getMapFromStr(feature.getString("properties"))
      } else mutable.Map.empty[String, Any]
      new Feature(geom, prop, id)
    }
    else if(jsonobject.getString("type")=="Feature"){
      val geom = if (jsonobject.containsKey("geometry")) { 
        OGEGeometry.geometry(jsonobject, crs)
      } else null
      val id = if (jsonobject.containsKey("id")) jsonobject.getString("id") else "" 
      val prop = if (jsonobject.containsKey("properties")) { 
        getMapFromStr(jsonobject.getString("properties"))
      } else mutable.Map.empty[String, Any]
      new Feature(geom, prop, id)
    }
    else {
      try {
        val ogeGeometryType = jsonobject.getString("type")
        val geom = OGEGeometry.getGeomFromCoors(jsonobject.toString, ogeGeometryType, true)
        geom.setSRID(OGEGeometry.crsToSRID(crs))
        new Feature(new OGEGeometry(geom), properties)
      } catch {
        case _: NoSuchElementException => throw new IllegalArgumentException("Only support geojson of Geometry, Feature or FeatureCollection.")
      }
    }
  }

  def load(featureId: String, productId: String, crs: String = "EPSG:4326"): Feature = {
    //Query a piece of data about a vector product from the database
    val (metaData, productKey, tableName, whereStore) = queryMetaData(productId)
    val feature = whereStore match{
      case 0 => { //Vector products are stored in h base
        val order = ""
        val rowKey = order+"_"+productKey+"_"+featureId //Currently, order+product key+feature id is used as row key in hbase
        HbaseServiceUtil.getFeatureWithRowkey(tableName, rowKey)
      }
      case 1 => { //Vector products are stored in postgresql (postgis)
        PostgresqlServiceUtil.queryFeature(featureId, tableName)
      }
    }
    feature.geometry.geometry.setSRID(crs.split(":")(1).toInt)
    feature
  }

  def loadFeatureFromUpload(featureId: String, userID: String, dagID: String, crs: String = "EPSG:4326"): Feature = {
    // Currently, only geojson file format is supported, because data in other formats are more suitable for importing into feature collections.
    var path: String = new String()
    if (featureId.endsWith(".geojson")) {
      path = s"${userID}/$featureId"
    } else {
      path = s"$userID/$featureId.geojson"
    }
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagID}_${Trigger.file_id}.geojson"
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    clientUtil.Download(path, filePath)
    println(s"Download $filePath")
    val source = Source.fromFile(filePath)
    val temp: String = source.mkString
    val feature = loadFromGeojson(temp, mutable.Map.empty[String, Any], crs)
    source.close()
    feature
  }

  def queryMetaData(productId: String): (ListBuffer[String], String, String, Int) = {
    //Query a piece of data about a vector product from the database
    val conn = PostgresqlUtilDev.getConnection
    val metaData = ListBuffer.empty[String]
    var tableName = ""
    var productKey = ""
    var whereStore = 0
    PostgresqlUtilDev.simpleSelect(
      resultNames = Array("oge_data_product.name", "oge_vector_fact.fact_data_ids", "oge_vector_fact.table_name", "oge_vector_fact.product_id", "oge_vector_fact.where_store"),
      tableName = "oge_vector_fact",
      jointLimit = Array(("oge_data_product", "oge_vector_fact.product_id = oge_data_product.id")),
      rangeLimit = Array(("name", "=", productId)),
      connection = conn,
      func = result => {
        while (result.next()) {
          val factDataIDs = result.getString("fact_data_ids")
          metaData.append(factDataIDs)
          productKey = result.getString("product_id")
          tableName = result.getString("table_name")
          whereStore = result.getInt("where_store")
        }
      }
    )
    conn.close()
    (metaData, productKey, tableName, whereStore)
  }

  def getMapFromStr(str: String): mutable.Map[String, Any] = {
    val map = mutable.Map.empty[String, Any]
    val keyValuePairs = str.stripPrefix("{").stripSuffix("}").split(',')
    keyValuePairs.foreach { pair =>
      val keyValue = pair.split(':')
      if (keyValue.length == 2) {
        val key = keyValue(0).trim
        val value = keyValue(1).trim
        map += (key -> value)
      }
      else if (keyValue.length == 1) {
        val key = keyValue(0).trim
        val value = null
        map += (key -> value)
      }
      else {
        val key = pair.substring(0, pair.lastIndexOf(":")).trim
        val value = pair.substring(pair.lastIndexOf(":")+1).trim
        map += (key -> value)
      }
    }
    map
  }


  def area(feature: Feature, crs: String = "EPSG:3857"): Double = {
    OGEGeometry.area(feature.geometry, crs)
  }

  private def templateFunc(feature: Feature, crs: String = "EPSG:4326", func: (OGEGeometry,String)=>OGEGeometry): Feature = {
    val newGeometry = func(feature.geometry, crs)
    new Feature(newGeometry, feature.properties, feature.id)
  }
  def bounds(feature: Feature, crs: String = "EPSG:4326"): Feature = templateFunc(feature, crs, OGEGeometry.bounds)

  def centroid(feature: Feature, crs: String = "EPSG:4326"): Feature = templateFunc(feature, crs, OGEGeometry.centroid)

  def convexHull(feature: Feature, crs: String = "EPSG:4326"): Feature = templateFunc(feature, crs, OGEGeometry.convexHull)

  def reproject(feature: Feature, tarCrsCode: String): Feature = templateFunc(feature, tarCrsCode, OGEGeometry.reproject)

  def buffer(feature: Feature, distance: Double, crs: String = null): Feature = {
    val newGeometry = OGEGeometry.buffer(feature.geometry, distance, crs)
    new Feature(newGeometry, feature.properties, feature.id)
  }

  def containedIn(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Boolean = {
    OGEGeometry.containedIn(feature1.geometry, feature2.geometry, crs)
  }

  def contains(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Boolean = {
    OGEGeometry.contains(feature1.geometry, feature2.geometry, crs)
  }

  //Returns a feature with the properties of the 'left' feature, and the geometry that results from subtracting the 'right' geometry from the 'left' geometry.
  def difference(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Feature = {
    val geometry = OGEGeometry.difference(feature1.geometry, feature2.geometry, crs)
    new Feature(geometry, feature1.properties, feature1.id)
  }

  def disjoint(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Boolean = {
    OGEGeometry.disjoint(feature1.geometry, feature2.geometry, crs)
  }

  def dissolve(feature: Feature, crs: String = "EPSG:4326"): Feature = {
    val geometry = OGEGeometry.dissolve(feature.geometry, crs)
    new Feature(geometry, feature.properties, feature.id)
  }

  def distance(feature1: Feature, feature2: Feature, crs: String = "EPSG:3857"): Double = {
    OGEGeometry.distance(feature1.geometry, feature2.geometry, crs)
  }

  def intersection(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Feature = {
    val geometry = OGEGeometry.intersection(feature1.geometry, feature2.geometry, crs)
    new Feature(geometry, feature1.properties, feature1.id)
  }

  def intersects(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Boolean = {
    OGEGeometry.intersects(feature1.geometry, feature2.geometry, crs)
  }

  def symmetricDifference(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Feature = {
    val geometry = OGEGeometry.symmetricDifference(feature1.geometry, feature2.geometry, crs)
    new Feature(geometry, feature1.properties, feature1.id)
  }

  def union(feature1: Feature, feature2: Feature, crs: String = "EPSG:4326"): Feature = {
    val geometry = OGEGeometry.union(feature1.geometry, feature2.geometry, crs)
    new Feature(geometry, feature1.properties, feature1.id)
  }

  def withinDistance(feature1: Feature, feature2: Feature, distance: Double, crs: String = "EPSG:4326"): Boolean = {
    OGEGeometry.withinDistance(feature1.geometry, feature2.geometry, distance, crs)
  }

  def id(feature: Feature): String = feature.id

  def length(feature: Feature, crs: String = "EPSG:3857"): Double = {
    OGEGeometry.length(feature.geometry, crs)
  }

  def perimeter(feature: Feature, crs: String = "EPSG:3857"): Double = {
    OGEGeometry.perimeter(feature.geometry, crs)
  }

  def propertyNames(feature: Feature): List[String] = {
    feature.properties.keys.toList
  }

  def copyProperties(destinationFeature: Feature, sourceFeature: Feature, properties: List[String] = null, exclude: List[String] = null): Feature = {
    var newProperties = destinationFeature.properties
    if (properties != null) {
      properties.foreach { prop =>
        if (sourceFeature.properties.contains(prop)) {
          newProperties = newProperties + (prop -> sourceFeature.properties(prop))
        }
      }
    } else {
      if (exclude != null) {
        sourceFeature.properties.foreach { case (key, value) =>
          if (!exclude.contains(key)) {
            newProperties = newProperties + (key -> value)
          }
        }
      } else {
        newProperties = newProperties ++ sourceFeature.properties
      }
    }
    new Feature(
      geometry = destinationFeature.geometry,
      properties = newProperties,
      id = destinationFeature.id
    )
  }

  def geometry(feature: Feature, geodesics: Option[Boolean] = None): String = {
    val geometry = feature.geometry
    geodesics match {
      case None => geometry.geometryToWKT()
      case Some(value) =>
        geometry.geodesic = value
        geometry.geometryToWKT()
    }
  }

  def get(feature: Feature, property: String): Any = {
    feature.properties.get(property) match {
      case None => null
      case Some(value) => value
    }
  }

  def getNumber(feature: Feature, property: String): Number = {
    feature.properties.get(property) match {
      case None => null
      case Some(value) =>
        value match {
          case num: Number => num
          case _ => null
        }
    }
  }

  def getString(feature: Feature, property: String): String = {
    feature.properties.get(property) match {
      case None => ""
      case Some(value) => value.toString
    }
  }

  def select(feature: Feature, propertySelectors: List[String], newProperties: List[String] = null, retainGeometry: Boolean = true): Feature = {
    if (newProperties.length != propertySelectors.length) throw new IllegalArgumentException("newProperties must match the number of propertySelectors")
    var selectedProperties: mutable.Map[String, Any] = mutable.Map()

    propertySelectors.zipWithIndex.foreach { case (selector, index) =>
      try {
        val regex: Regex = selector.r
        feature.properties.foreach { case (key, value) =>
          if (regex.findFirstIn(key).isDefined) {
            val newKey = if (newProperties != null) newProperties(index) else key
            selectedProperties = selectedProperties + (newKey -> value)
          }
        }
      } catch {
        case _: MatchError =>
          if (feature.properties.contains(selector)) {
            val newKey = if (newProperties != null) newProperties(index) else selector
            selectedProperties = selectedProperties + (newKey -> feature.properties(selector))
          }
      }
    }

    val newGeometry = if (retainGeometry) feature.geometry else null

    new Feature(
      geometry = newGeometry,
      properties = selectedProperties,
      id = feature.id
    )
  }

  def set(feature: Feature, var_args: Any*): Feature = {
    var newProperties = feature.properties

    if (var_args.size == 1 && var_args.head.isInstanceOf[mutable.Map[_, _]]) {
      val inputMap = var_args.head.asInstanceOf[mutable.Map[String, Any]]
      newProperties = newProperties ++ inputMap
    } else if (var_args.size % 2 == 0) {
      val inputMap = var_args.grouped(2).collect {
        case Seq(key: String, value) => key -> value
      }.toMap
      newProperties = newProperties ++ inputMap
    }
    else throw new IllegalArgumentException("var_args is either a dictionary of properties, or a vararg sequence of properties, e.g. key1, value1, key2, value2, ...(The quantity is an even number)")

    new Feature(
      geometry = feature.geometry,
      properties = newProperties,
      id = feature.id
    )
  }

  def setGeometry(feature: Feature, geometry: OGEGeometry = null): Feature = {
    new Feature(
      geometry = geometry,
      properties = feature.properties,
      id = feature.id
    )
  }

  def simplify(feature: Feature, distanceTolerance: Double, crs: String = "EPSG:4326", topoPreserved: Boolean = false): Feature = {
    new Feature(
      geometry = OGEGeometry.simplify(feature.geometry, distanceTolerance, crs, topoPreserved),
      properties = feature.properties,
      id = feature.id
    )
  }

  def densify(feature: Feature, distanceTolerance: Double, crs: String = "EPSG:4326"): Feature = {
    new Feature(
      geometry = OGEGeometry.densify(feature.geometry, distanceTolerance, crs),
      properties = feature.properties,
      id = feature.id
    )
  }

  def toArray(feature: Feature, properties: List[String]): Array[Any] = {
    properties.map( prop =>
      feature.properties.get(prop) match {
        case Some(value: Any) => value
        case _ => null
      }
    ).toArray
  }

  def toDictionary(feature: Feature, properties: List[String] = null): mutable.Map[String, Any] = {
    if (properties==null) feature.properties
    else {
      var map: mutable.Map[String, Any] = mutable.Map()
      properties.foreach( prop =>
        feature.properties.get(prop) match {
          case Some(value: Any) => map += (prop -> value)
          case _ => map += (prop -> null)
        }
      )
      map
    }
  }

  def cutLines(feature: Feature, distances: List[Double], crs: String = "EPSG:4326"): Feature = {
    new Feature(
      geometry = OGEGeometry.cutLines(feature.geometry, distances, crs),
      properties = feature.properties,
      id = feature.id
    )
  }

  def visualize(implicit sc: SparkContext, feature: Feature, color:List[String], attribute:String): Unit = {
    val featureCollection = FeatureCollection.loadFromFeature(sc, feature)
    FeatureCollection.visualize(featureCollection, color, attribute)
  }

  def closestPoint(feature1: Feature, feature2: Feature, terminateDistance: Double = 0.0): Feature = {
    val geo = OGEGeometry.closestPoint(feature1.geometry, feature2.geometry, terminateDistance)
    val result = mutable.Map[String, Any](
      "method" -> "closestPoint"
    )
    new Feature(geo, result)
  }

  def closestPoints(feature1: Feature, feature2: Feature, terminateDistance: Double = 0.0): Feature = {
    val pointMap: mutable.Map[String, OGEGeometry] =
      OGEGeometry.closestPoints(feature1.geometry, feature2.geometry, terminateDistance)
    val coordinates = pointMap.values.map(_.geometry.getCoordinate).toArray
    val factory = feature1.geometry.geometry.getFactory
    val multiPoint = factory.createMultiPointFromCoords(coordinates)
    val result = mutable.Map[String, Any](
      "method" -> "closestPoints"
    )
    new Feature(new OGEGeometry(multiPoint), result)
  }

  def exportFeature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))]
                    , fileType: String = "geojson", batchParam: BatchParam, dagId: String): RDD[(String, (Geometry, mutable
  .Map[String,
    Any]))] = {

    implicit val formats = DefaultFormats

    def geometryToJson(geometry: Geometry): Map[String, Any] = geometry match {
      case point: Point => mutable.Map("type" -> "Point", "coordinates" -> List(point.getX, point.getY))
      case line: LineString => mutable.Map("type" -> "LineString", "coordinates" -> line.getCoordinates.map(c => List(c.x, c.y)).toList)
      case polygon: Polygon =>
        val exterior = polygon.getExteriorRing.getCoordinates.map(c => List(c.x, c.y)).toList
        val interiors = (0 until polygon.getNumInteriorRing).map(i =>
          polygon.getInteriorRingN(i).getCoordinates.map(c => List(c.x, c.y)).toList
        ).toList
        Map("type" -> "Polygon", "coordinates" -> (exterior :: interiors))

      case multiPolygon: MultiPolygon =>
        val polygons = (0 until multiPolygon.getNumGeometries).map(i => {
          val poly = multiPolygon.getGeometryN(i).asInstanceOf[Polygon]
          val exterior = poly.getExteriorRing.getCoordinates.map(c => List(c.x, c.y)).toList
          val interiors = (0 until poly.getNumInteriorRing).map(j =>
            poly.getInteriorRingN(j).getCoordinates.map(c => List(c.x, c.y)).toList
          ).toList
          exterior :: interiors
        }).toList
        Map("type" -> "MultiPolygon", "coordinates" -> polygons)

      case _ => throw new IllegalArgumentException("Unsupported geometry type")
    }

    if (fileType == "geojson") {
      val features = feature.collect().map { case (id, (geometry, properties)) =>
        val geometryJson = geometryToJson(geometry)
        val scalaMap = mutable.Map[String, Any]()
        if (properties.isInstanceOf[JSONObject]) {
          properties.asInstanceOf[JSONObject].forEach { (key, value) => scalaMap(key) = value }
        }

        Map(
          "type" -> "Feature",
          "id" -> id,
          "geometry" -> geometryJson,
          "properties" -> scalaMap
        )
      }.toList
      //  FeatureCollection
      val featureCollection = Map(
        "type" -> "FeatureCollection",
        "features" -> features
      )
      val jsonString = Serialization.write(featureCollection)

      val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.geojson"
      val fileWriter = new FileWriter(saveFilePath)
      fileWriter.write(jsonString)
      fileWriter.close()

      val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
      val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
      val obj: JSONObject = new JSONObject
      obj.put("path", path.toString)
      PostSender.shelvePost("info", obj)
      clientUtil.Upload(path, saveFilePath)
      feature
    }
    feature
  }
}
