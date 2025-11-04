package whu.edu.cn.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.geotools.data.FeatureWriter
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.opengis.feature.simple.SimpleFeatureType

import java.io.{File, FileInputStream, IOException}

object FeatureCollectionUtil {

  //递归检查目录下是否存在Shapefile文件
  def findCompleteShapefile(dir: File): String = {
    val requiredExtensions = Set(".shp", ".dbf", ".shx")
    // 首先检查当前目录
    val files = dir.listFiles()
    val shpFiles = files.filter(f => f.isFile && f.getName.endsWith(".shp"))
    if (shpFiles.nonEmpty) {
      // 当前目录有.shp文件，检查是否完整
      val baseName = shpFiles.head.getAbsolutePath.stripSuffix(".shp")
      val missingFiles = requiredExtensions.filterNot(ext => new File(baseName + ext).exists())
      if (missingFiles.isEmpty) {
        return dir.getPath()  // 返回包含完整Shapefile的目录
      }
    }
    // 如果没有找到，递归检查子目录
    val subDirs = files.filter(_.isDirectory)
    for (subDir <- subDirs) {
      try {
        return findCompleteShapefile(subDir)
      } catch {
        case _: IllegalArgumentException => // 忽略，继续检查其他子目录
      }
    }
    // 所有子目录都检查过了，仍然没找到
    throw new IllegalArgumentException(
      s"在目录 ${dir.getAbsolutePath} 及其子目录中未找到完整的Shapefile（需要.shp、.dbf、.shx文件）"
    )
  }

  /**
   * 将前缀相同(名字)的矢量文件组织到(shp,dbf,shx...)一个文件中
   * @param dir have a directory with many Shapefiles
   * @return temp file
   */
  def shapeFilesAggregate(dir: File): Seq[String] = {
    val shapeFileGroups = dir.listFiles()
      .filter(_.isFile)
      .groupBy { file =>
        file.getName.split("\\.", 2).head // 按前缀分组
      }
    // 用于存储新创建的文件夹路径
    val createdFolders = scala.collection.mutable.ArrayBuffer[String]()
    // 遍历分组并处理
    shapeFileGroups.foreach { case (prefix, files) =>
      // 创建新文件夹路径（在dir下）
      val newFolder = new File(dir.getAbsolutePath, prefix)
      if (!newFolder.exists()) {
        newFolder.mkdir() // 创建文件夹
        createdFolders += newFolder.getAbsolutePath
      }
      // 将分组中的文件移动到新文件夹
      files.foreach { file =>
        val newFilePath = new File(newFolder, file.getName)
        file.renameTo(newFilePath) // 移动文件
      }
    }
    // 返回所有创建的文件夹路径
    createdFolders
  }

  /**
    * 生成shp
    *
    * @param featureCollection SimpleFeatureCollection
    * @param path              输出路径 shp结尾
    * @throws IOException e
    */
//  def buildShp(featureCollection: SimpleFeatureCollection, path: String): Unit = {
//    val newDataStore: ShapefileDataStore = getShpDataStore(path)
//    val type_: SimpleFeatureType = featureCollection.getSchema()
//
//  }

//  @throws Exception
//  def buildShp(featureCollection: SimpleFeatureCollection, path: String): Unit {
//    // 创建输出数据存储
//    val newDataStore: ShapefileDataStore = getShpDataStore(path)
//    val `type`: SimpleFeatureType  = featureCollection.getSchema()
//
//    // 设置输出数据存储
//    newDataStore.createSchema(type);
//    // 设置坐标系
//    newDataStore.forceSchemaCRS(type.getCoordinateReferenceSystem());
//
//    addFeatureTransaction(newDataStore, featureCollection);
//  }
  /**
    * 若文件存在，返回该shp；若不存在，创建新dataStore
    *
    * @param path 文件路径
    * @return ShapefileDataStore
    * @throws IOException e
    */
//  def getShpDataStore(path: String): ShapefileDataStore{
//    val newFile: File = new File(path)
//    ShapefileDataStore inputDataStore;
//    if (newFile.exists()) {
//    inputDataStore = (ShapefileDataStore) DataStoreFinder.getDataStore(
//    Collections.singletonMap("url", newFile.toURI().toURL()));
//  } else {
//    ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
//    inputDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(
//    Collections.singletonMap("url", newFile.toURI().toURL()));
//  }
//    inputDataStore.setCharset(getShapeFileCharsetName(path));
//    return inputDataStore;
//
//  }

  import org.geotools.data.shapefile.ShapefileDataStore
  import org.geotools.data.{FileDataStoreFinder, Transaction}
  import org.geotools.geojson.geom.GeometryJSON
  import org.opengis.feature.simple.SimpleFeature

  import java.io.InputStream
  import scala.collection.JavaConverters._

  def geoJSON2Shp(geoJson: File, path: String): Unit = {
    val in: InputStream = new FileInputStream(geoJson)
    geoJSON2Shp(in, path)
  }

  def geoJSON2Shp(input: InputStream, path: String): Unit = {
    val fjson = new FeatureJSON(new GeometryJSON(15))
    val jsonIt = fjson.streamFeatureCollection(input)
    if (!jsonIt.hasNext) {
      throw new IllegalArgumentException("Cannot create shapefile. GeoJSON stream is empty")
    }
    var writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = null
    var shpDataStore: ShapefileDataStore = null
    try {
      // 创建输出数据存储
      shpDataStore = FileDataStoreFinder.getDataStore(new java.io.File(path)).asInstanceOf[ShapefileDataStore]
      // 使用第一个要素的类型创建Schema
      val firstFeature = jsonIt.next()
      shpDataStore.createSchema(firstFeature.getFeatureType)
      // 获取FeatureWriter用于追加要素
      writer = shpDataStore.getFeatureWriterAppend(shpDataStore.getTypeNames()(0), Transaction.AUTO_COMMIT)
      // 添加第一个要素
      addFeature(firstFeature, writer)
      // 循环添加剩余要素
      while (jsonIt.hasNext) {
        val feature = jsonIt.next()
        addFeature(feature, writer)
      }
    } finally {
      // 确保资源关闭
      if (writer != null) writer.close()
      if (jsonIt != null) jsonIt.close()
    }
  }

  /**
    * 将单个要素添加到Writer
    */
  private def addFeature(feature: SimpleFeature, writer: FeatureWriter[SimpleFeatureType, SimpleFeature]): Unit = {
    val featureType = writer.getFeatureType
    val newFeature = writer.next()
    // 复制属性
    featureType.getAttributeDescriptors.asScala.foreach(descriptor => {
      val name = descriptor.getLocalName
      if (feature.getAttribute(name) != null) {
        newFeature.setAttribute(name, feature.getAttribute(name))
      }
    })
    writer.write()
  }

  def stringToRow(dataString: String, schema: StructType): Row = {
    // 按制表符分割字符串
    val fields = dataString.split("\t")

    // 验证字段数与Schema匹配
    if (fields.length != schema.fields.length) {
      throw new IllegalArgumentException(
        s"字段数量不匹配: 期望 ${schema.fields.length}, 实际 ${fields.length}\n" +
          s"数据内容: $dataString"
      )
    }

    // 转换每个字段为指定类型
    val values = fields.zip(schema.fields).map { case (field, structField) =>
      if (field.trim.toLowerCase == "null") {
        // 处理"null"字符串
        null
      } else {
        structField.dataType match {
          // 整数类型
          case IntegerType =>
            try { field.toInt } catch { case _: Exception => null }
          // 长整型
          case LongType =>
            try { field.toLong } catch { case _: Exception => null }
          // 双精度浮点型
          case DoubleType =>
            try { field.toDouble } catch { case _: Exception => null }
          // 浮点型
          case FloatType =>
            try { field.toFloat } catch { case _: Exception => null }
          case _: DecimalType =>
            try { new java.math.BigDecimal(field.trim) } catch { case _: Exception => null }
          // 布尔型
          case BooleanType =>
            field.trim.toLowerCase match {
              case "true" | "1" => true
              case "false" | "0" => false
              case _ => null
            }
          case GeometryUDT =>
            try {
              // 假设field是WKT格式的几何字符串
              val reader = new org.locationtech.jts.io.WKTReader()
              reader.read(field)
            } catch {
              case e: Exception =>
                println(s"无法解析几何字符串: $field, 错误: ${e.getMessage}")
                null
            }
          // 日期类型 (简化处理)
          case DateType =>
            try { java.sql.Date.valueOf(field.trim) } catch { case _: Exception => null }
          // 字符串类型
          case StringType =>
            if (field.trim.isEmpty) null else field
          // 其他类型按字符串处理
          case _ => field
        }
      }
    }
    // 创建带Schema的Row实例
    new GenericRowWithSchema(values.toArray, schema)
  }

  /**
   * Get / create a valid version of the geometry given. If the geometry is a polygon or multi polygon, self intersections /
   * inconsistencies are fixed. Otherwise the geometry is returned.
   *
   * @param geom the input geometry
   * @return a valid geometry
   */
  def validate(geom: Geometry): Geometry = {
    geom match {
      case polygon: Polygon =>
        if (polygon.isValid) {
          polygon.normalize() // validate does not pick up rings in the wrong order - this will fix that
          polygon // If the polygon is valid just return it
        } else {
          val polygonizer = new Polygonizer()
          addPolygon(polygon, polygonizer)
          toPolygonGeometry(polygonizer.getPolygons.asScala.toSet.map((i:Any) => i.asInstanceOf[Polygon]), polygon.getFactory)
        }
      case multiPolygon: MultiPolygon =>
        if (multiPolygon.isValid) {
          multiPolygon.normalize() // validate does not pick up rings in the wrong order - this will fix that
          multiPolygon // If the multipolygon is valid just return it
        } else {
          val polygonizer = new Polygonizer()
          for (n <- multiPolygon.getNumGeometries - 1 to 0 by -1) {
            addPolygon(multiPolygon.getGeometryN(n).asInstanceOf[Polygon], polygonizer)
          }
          toPolygonGeometry(polygonizer.getPolygons.asScala.toSet.map((i:Any) => i.asInstanceOf[Polygon]), multiPolygon.getFactory)
        }
      case _ =>
        geom // Only care about polygon / multipolygon geometries
    }
  }

  /**
   * Add all line strings from the polygon given to the polygonizer given
   *
   * @param polygon polygon from which to extract line strings
   * @param polygonizer polygonizer
   */
  private def addPolygon(polygon: Polygon, polygonizer: Polygonizer): Unit = {
    addLineString(polygon.getExteriorRing, polygonizer)
    for (n <- polygon.getNumInteriorRing - 1 to 0 by -1) {
      addLineString(polygon.getInteriorRingN(n), polygonizer)
    }
  }

  /**
   * Add the linestring given to the polygonizer
   *
   * @param lineString line string
   * @param polygonizer polygonizer
   */
  private def addLineString(lineString: LineString, polygonizer: Polygonizer): Unit = {
    val ls = lineString match {
      case linearRing: LinearRing =>
        // LinearRings are treated differently to line strings: we need a LineString NOT a LinearRing
        lineString.getFactory.createLineString(lineString.getCoordinateSequence)
      case _ => lineString
    }

    // Unioning the linestring with the point makes any self intersections explicit.
    val point = ls.getFactory.createPoint(ls.getCoordinateN(0))
    val toAdd = ls.union(point)

    // Add result to polygonizer
    polygonizer.add(toAdd)
  }

  /**
   * Get a geometry from a collection of polygons.
   *
   * @param polygons collection of polygons
   * @param factory factory to generate MultiPolygon if required
   * @return null if there were no polygons, the polygon if there was only one, or a MultiPolygon containing all polygons otherwise
   */
  private def toPolygonGeometry(polygons: Set[Polygon], factory: GeometryFactory): Geometry = {
    polygons.size match {
      case 0 => null // No valid polygons!
      case 1 => polygons.head // Single polygon - no need to wrap
      case _ =>
        // Polygons may still overlap! Need to sym difference them
        var ret: Geometry = polygons.head
        for (polygon <- polygons.tail) {
          ret = ret.symDifference(polygon)
        }
        ret
    }
  }

  }
