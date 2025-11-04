package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{DoubleCellType, MultibandTile, RasterExtent, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.raster.interpolation.{InverseDistanceWeighted, SimpleKrigingMethods}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.vector
import geotrellis.vector.{Extent, PointFeature}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{Envelope, Geometry, Point}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity.{BatchParam, OGEGeometryType, SpaceTimeBandKey}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util._

import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
import org.apache.sedona
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.{Adapter, GeometrySerializer, SedonaSQLRegistrator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import whu.edu.cn.oge.Feature.{getMapFromStr, queryMetaData}

import java.util
import java.io.File
import scala.collection.JavaConverters._
import java.io.{BufferedWriter, Closeable, FileWriter}
import com.alibaba.fastjson.serializer.{ObjectSerializer, SerializerFeature}
import geotrellis.proj4.CRS
import geotrellis.vector.interpolation.{NonLinearSemivariogram, Semivariogram, Spherical}
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.util.GeometryFixer
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.operation.buffer.BufferOp
import whu.edu.cn

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.math.{max, min}
//import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileUtil, Path}

import scala.collection.mutable
import scala.language.implicitConversions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import org.apache.spark.sql.sedona_sql.expressions.st_aggregates._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.types._
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator

import whu.edu.cn.util.FeatureCollectionUtil
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.DbfFileReader
import org.apache.spark.sql.sedona_sql.expressions.ST_GeomFromText
import org.locationtech.jts.io.WKBWriter
import org.apache.sedona.sql.utils.Adapter
import java.sql.Date
import scala.util.hashing.MurmurHash3

class FeatureCollection(val dataFrame: DataFrame, var geometryFieldName: String = "geom") extends Serializable {
  val type_ : String  = "FeatureCollection"
  var fieldNames: Seq[String] = dataFrame.columns
  lazy val spatialRDD: SpatialRDD[Geometry] = Adapter.toSpatialRdd(dataFrame, geometryFieldName, fieldNames)
}
object FeatureCollection {

  def loadFromGeometry(implicit sc: SparkContext, geometry: OGEGeometry, properties: Map[String, Any] = Map.empty[String, Any], id: String = "0"): DataFrame={
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    val geom = geometry.geometry
    val idField = StructField("id", StringType, nullable = true)
    val geomField = StructField("geom", GeometryUDT, nullable = true)
    val fields: Seq[StructField] = properties.map { case (key, value) =>
      value match {
        case _: String => StructField(key, StringType, nullable = true)
        case _: Int => StructField(key, IntegerType, nullable = true)
        case _: Double => StructField(key, DoubleType, nullable = true)
        // Add more cases as needed for other types
        case _ => StructField(key, StringType, nullable = true) // Default to StringType
      }
    }.toSeq
    val schema = StructType(idField +: geomField +: fields)
    val row = Row.fromSeq(id +: geom +: properties.values.toSeq)
    spark.createDataFrame(List(row).asJava, schema)
  }

  def loadFromFeature(implicit sc: SparkContext, feature: Feature): DataFrame = { //多个重载
    val geometry: OGEGeometry = feature.geometry
    val id: String = feature.id
    val properties: mutable.Map[String, Any] = feature.properties
    loadFromGeometry(sc, geometry, properties, id)
  }

  def loadFromFeatureList(implicit sc: SparkContext, featureList: List[Feature]): DataFrame = {
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    val allKeys = featureList.flatMap(_.properties.keys).distinct
    val idField = StructField("id", StringType, nullable = true)
    val geomField = StructField("geom", GeometryUDT, nullable = true)
    val fields: Seq[StructField] = allKeys.map { key =>
      val firstValue = featureList.flatMap(_.properties.get(key)).find(f => f != None)
      firstValue match {
        case Some(_: String) => StructField(key, StringType, nullable = true)
        case Some(_: Int) => StructField(key, IntegerType, nullable = true)
        case Some(_: Double) => StructField(key, DoubleType, nullable = true)
        // Add more cases as needed for other types
        case _ => StructField(key, StringType, nullable = true) // Default to StringType
      }
    }
    val schema = StructType(idField +: geomField +: fields)
    val rowList = featureList.map { feature =>
      val geom: Geometry = feature.geometry.geometry
      val id: String = feature.id
      val properties: Map[String, Any] = feature.properties
      val values: List[Any] = allKeys.map { key =>
        properties.getOrElse(key, null)
      }
      Row.fromSeq(id +: geom +: values)
    }
    spark.createDataFrame(rowList.asJava, schema)
  }

  def loadFromGeojson(implicit sc: SparkContext, gjson: String, crs: String = "EPSG:4326"): DataFrame = {
    val escapedJson = gjson.replace("\\", "")//去除转义符
    val jsonobject: JSONObject = JSON.parseObject(escapedJson)
    var df: DataFrame = null
    if (jsonobject.getString("type")=="FeatureCollection") {
      val featureArray: JSONArray = jsonobject.getJSONArray("features")
      val featureList: ListBuffer[Feature] = ListBuffer.empty[Feature]
      for (i <- 0 until featureArray.size()){
        val feature: JSONObject = featureArray.getJSONObject(i)
        val geom: OGEGeometry =
          if (feature.containsKey("geometry")) { //如果存在geometry，则将其作为属性
          OGEGeometry.geometry(feature, crs)
        } else null
        val properties =
          if (feature.containsKey("properties")) { //如果存在properties，则将其作为属性
          Feature.getMapFromStr(feature.getString("properties"))
        } else null
        val feat = new Feature(geom, properties)
        feat.id = if (feature.containsKey("id")) feature.getString("id")  else null //如果存在id，则将其作为属性
        featureList.append(feat)
      }
      df = loadFromFeatureList(sc, featureList.toList)
    }
    else if(jsonobject.getString("type")=="Feature"){
      val geom: OGEGeometry =
        if (jsonobject.containsKey("geometry")) { //如果存在geometry，则将其作为属性
        OGEGeometry.geometry(jsonobject, crs)
      } else null
      val properties =
        if (jsonobject.containsKey("properties")) { //如果存在properties，则将其作为属性
        getMapFromStr(jsonobject.getString("properties"))
      } else null
      val feat = new Feature(geom, properties)
      feat.id = if (jsonobject.containsKey("id")) jsonobject.getString("id") else null //如果存在id，则将其作为属性
      df = loadFromFeature(sc, feat)
    }
    else {
      try {
        val ogeGeometryType = jsonobject.getString("type")
        val geom = OGEGeometry.getGeomFromCoors(jsonobject.toString, ogeGeometryType, true)
        geom.setSRID(OGEGeometry.crsToSRID(crs))
        df = loadFromGeometry(sc, new OGEGeometry(geom))
      } catch {
        case _: NoSuchElementException => println("Only geojson of Geometry, Feature or FeatureCollection type is supported.")
      }
    }
    df
  }
  
  def load(implicit sc: SparkContext,
           productId: String,
           extent: OGEGeometry = null, startTime: String = null, endTime: String = null,
           crs: String = null): DataFrame = {

    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    val (metaData, productKey, tableName, whereStore) = queryMetaData(productId)
    var featureCollection = whereStore match{
      case 0 => //store in HBase
        HbaseServiceUtil.getFeatureCollection(spark, tableName, productKey, extent, startTime, endTime)
      case 1 => //store in PostgreSQL(PostGIS)
        PostgresqlServiceUtil.queryFeatureCollection(spark, tableName, extent, startTime, endTime)
    }
    if(crs != null){
      val srid = featureCollection.head().getAs[Geometry]("geom").getSRID
      if (srid != crs.split(":")(1).toInt)
        featureCollection = reproject(sc, featureCollection, crs)
    }
    featureCollection.filter(expr("ST_IsValid(geom)")).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def loadFeatureCollectionFromUpload(implicit sc: SparkContext,
                                      featureId: String, userID: String, dagID: String,
                                      crs: String = "EPSG:4326"): DataFrame = {
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    // 支持多种文件格式，因此要求用户指定清楚文件后缀
    if (featureId.split("\\.").length==1) {
      throw new IllegalArgumentException("Please specify the file format.")
    }
    val path: String = s"${userID}/$featureId"
    val format = featureId.split("\\.").last
    val supportedFormat = List("csv", "json", "geojson", "parquet", "shp", "zip")
    if (!supportedFormat.contains(format)) throw new IllegalArgumentException(s"The file format $format is not supported.")
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagID}_${Trigger.file_id}.$format"
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    clientUtil.Download(path, filePath)
    println(s"Download $filePath")

    val df: DataFrame = format match {
      //CSV文件第一列作为列名，数据类型保持不变
      case "csv" => {
        spark.read.option("header", value = true).option("inferSchema", value = true)
          .csv(filePath) //.option("delimiter","\t") 改变分隔符
          .withColumn("geom", expr("ST_GeomFromText(geom)"))
      }
      case "json" => spark.read.json(filePath)
      case "parquet" => spark.read.parquet(filePath).withColumnRenamed("geometry", "geom")
      case "geojson" => {
        val source = Source.fromFile(filePath)
        val temp: String = source.mkString
        source.close()
        loadFromGeojson(sc, temp, crs)
        //SedonaUtil.geoJsonToDF(sc,filePath)
      }
      case "shp" => {
        try {
          clientUtil.Download(path.replace(".shp", ".dbf"), filePath.replace(".shp", ".dbf"))
          clientUtil.Download(path.replace(".shp", ".prj"), filePath.replace(".shp", ".prj"))
          clientUtil.Download(path.replace(".shp", ".shx"), filePath.replace(".shp", ".shx"))
        }
        catch {
          case _: Exception => throw new IllegalArgumentException("The .dbf, .prj, .shx files are also required along with the .shp file.")
        }
        SedonaUtil.shpToDF(sc, tempPath)
      }
    }
    df
  }

  //GEE函数接口实现
  //（GEE中有一些Collection，范围更广？ 后面统一一下参数是叫features还是collection 有时候变量类型是Element有时是computedObject关注前端）
  // （GEE中还有些经常在参数中使用的类，比如ErrorMargin Projection Reducer后面可以考虑实现）
//  def randomPoints(implicit sc: SparkContext, region: OGEGeometry, points: Int = 1000, seed: Long = 0): DataFrame={
//    import org.apache.spark.sql.sedona_sql.expressions.raster.RS_AsRaster
//    import org.apache.spark.sql.sedona_sql.expressions.raster.RS_SRID
//    RS_AsRaster(region.geometry)
//    val maxDimension = region.geometry.getDimension
//    for(i <- 0 until region.geometry.getNumGeometries) {
//      val geom = region.geometry.getGeometryN(i)
//      if (geom.getDimension == maxDimension)
//    }
//  }

  def loadAllTableElements(implicit sc: SparkContext,
                           table: String, geometryColumn: String = null): DataFrame = {
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    if (geometryColumn == null) {
      PostGisUtil.loadFromPostGISWithoutGeometry(spark, table)
    }
    else PostGisUtil.loadFromPostGIS(spark,
      table, geometryColumn)
  }

  def loadWithRestrictions(implicit sc: SparkContext,
                           query: String, geometryColumn: String = null): DataFrame = {
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    if (geometryColumn==null) {
      PostGisUtil.selectUsingSQLWithoutGeometry(spark,
        query.replace("\"", "").replace("\'", ""))
    }
    else PostGisUtil.selectUsingSQL(spark,
      query, geometryColumn)
  }

  //融合成仅包含一个Feature一个FeatureCollection，这个Feature仅有一个名为“union_result"的ID及一个几何图形
  def aggregate_union(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    if (!collection.columns.contains(geometryFieldName)) throw new IllegalArgumentException("The geometry field does not exist.")
    val aggGeometryColumn: Column = ST_Union_Aggr(collection.col(geometryFieldName))
    val resultDF: DataFrame = collection.agg(aggGeometryColumn.as(geometryFieldName))
    resultDF.withColumn("id", lit("union_result"))
    resultDF
  }

  //Return the polygon intersection of all polygons in A
  def aggregate_intersection(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    if (!collection.columns.contains(geometryFieldName)) throw new IllegalArgumentException("The geometry field does not exist.")
    val aggGeometryColumn: Column = ST_Intersection_Aggr(collection.col(geometryFieldName))
    val resultDF: DataFrame = collection.agg(aggGeometryColumn.as(geometryFieldName))
    resultDF.withColumn("id", lit("union_result"))
    resultDF
  }

  def merge(collection1: DataFrame, collection2: DataFrame): DataFrame = {
    // 检查是否存在id字段，不存在则生成唯一标识
    val df1 = if (collection1.columns.contains("id")) {
      collection1.withColumn("id", concat(lit("1_"), col("id")))
    } else {
      collection1.withColumn("id", concat(lit("1_"), monotonically_increasing_id()))
    }
    val df2 = if (collection2.columns.contains("id")) {
      collection2.withColumn("id", concat(lit("2_"), col("id")))
    } else {
      collection2.withColumn("id", concat(lit("2_"), monotonically_increasing_id()))
    }
    // 获取两个DF的所有列名
    val allColumns = (df1.columns ++ df2.columns).distinct
    // 构建两个DF，确保它们拥有相同的列集，缺失的列用null填充
    val expandedDf1 = allColumns.foldLeft(df1) { (df, colName) =>
      if (df.columns.contains(colName)) df else df.withColumn(colName, lit(null))
    }
    val expandedDf2 = allColumns.foldLeft(df2) { (df, colName) =>
      if (df.columns.contains(colName)) df else df.withColumn(colName, lit(null))
    }
    // 执行合并
    expandedDf1.unionByName(expandedDf2) //基于列名合并
  }

  def mergeAll(collectionList: List[DataFrame]): DataFrame = {
    if (collectionList.isEmpty) {
      // 处理空列表的情况
      throw new IllegalArgumentException("List of DataFrames cannot be empty")
    }
    // 为每个DataFrame添加唯一标识前缀
    val processedDfs = collectionList.zipWithIndex.map { case (df, index) =>
      val prefix = s"${index + 1}_"
      if (df.columns.contains("id")) {
        df.withColumn("id", concat(lit(prefix), col("id")))
      } else {
        df.withColumn("id", concat(lit(prefix), monotonically_increasing_id()))
      }
    }
    // 获取所有DataFrame的所有列名
    val allColumns = processedDfs.flatMap(_.columns).distinct
    // 扩展每个DataFrame，确保它们拥有相同的列集
    val expandedDfs = processedDfs.map { df =>
      allColumns.foldLeft(df) { (currentDf, colName) =>
        if (currentDf.columns.contains(colName)) currentDf else currentDf.withColumn(colName, lit(null))
      }
    }
    // 合并所有DataFrame
    expandedDfs.reduce(_ unionByName _)
  }

  // 创建一个可序列化的UDF类，避免闭包捕获问题
  class GeometryPreprocessor extends Serializable {
    def processGeometry(geom: Geometry): Geometry = {
      if (geom == null) return null

      try {
        // 通过缓冲区0操作强制节点化，解决非节点化交集问题
        BufferOp.bufferOp(geom, 0.0)
      } catch {
        case e: Exception =>
          println(s"Geometry preprocessing failed: ${e.getMessage}")
          null
      }
    }
  }
  class GeometryPreprocessor1 extends Serializable {
    def processGeometry(geom: Geometry): Geometry = {
      if (geom == null) return null
      try {
        FeatureCollectionUtil.validate(geom)
      } catch {
        case e: Exception =>
          println(s"Geometry preprocessing failed: ${e.getMessage}")
          null
      }
    }
  }

  def geometryGetAndMerge(collection: DataFrame, geometryFieldName: String = "geom"): OGEGeometry = {
    if (!collection.columns.contains(geometryFieldName)) throw new IllegalArgumentException("The geometry field does not exist.")
    //要求集合内所有几何图形必须采用相同的投影坐标系和边界处理规则(edge interpretation)
    // 创建UDF实例并注册为可序列化的函数
    val processor = new GeometryPreprocessor()
    // 使用lambda表达式调用processor的方法，而不是在lambda中直接实现逻辑
    val preprocessGeometry: UserDefinedFunction = udf[Geometry, Geometry](
      geom => processor.processGeometry(geom)
    )

    // 对几何字段进行预处理
    val preprocessedDF = collection.withColumn(
      "preprocessed_geom",
      preprocessGeometry(col(geometryFieldName))
    )
    // 执行聚合Union操作
    val aggGeometryColumn: Column = ST_Union_Aggr(col("preprocessed_geom"))
    val resultDF: DataFrame = preprocessedDF.agg(aggGeometryColumn.as(geometryFieldName))
    // 获取结果几何
    val geom = resultDF.first().getAs[Geometry](geometryFieldName)
    new OGEGeometry(geom)
  }
//  def dataTypeToScalaType(dataType: DataType) = {
//    dataType match {
//      case _: IntegerType => Int
//      case _: LongType => Long
//      case _: DoubleType => Double
//      case _: FloatType => Float
//      case _: ShortType => Short
//      case _: ByteType => Byte
//      case _: BooleanType => Boolean
//      case _: StringType =>
//      case _: CharType => Char
//      case _ => throw new IllegalArgumentException("The data type is not supported to convert to scala class.")
//    }
//  }
  //不存在列，或不为数值类型时，返回空值？
  def aggregate_template(collection: DataFrame, property: String, aggregateFunction: (String=>Column)): Number = {
    val propertyCol =
      try{
        collection.select(property)
      } catch {
        case _: Throwable => throw new IllegalArgumentException("The property does not exist.")}
    propertyCol.schema.fields(0).dataType match {
      case _: NumericType => propertyCol.agg(aggregateFunction(property)).first().getAs[Number](property)
      case _ => throw new IllegalArgumentException("The property is not a numeric type.")
    }
  }
  def aggregate_min(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, functions.min)
  }
  def aggregate_mean(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, mean)
  }
  def aggregate_max(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, functions.max)
  }
  def aggregate_product(collection: DataFrame, property: String): Number = {
    val productFunction: (String=>Column) = {x: String => expr(s"AGGREGATE(filter(${x}, x -> x IS NOT NULL AND x != 0), 1.0D, (acc, x) -> acc * x)")}
    aggregate_template(collection, property, productFunction)
  }
  def aggregate_sample_sd(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, stddev_samp) //stddev_samp是样本标准差，stddev_pop是总体标准差
  }
  def aggregate_sample_var(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, var_samp) //var_samp是样本方差，var_pop是总体方差
  }
  def aggregate_total_sd(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, stddev_pop) //var_samp是样本方差，var_pop是总体方差
  }
  def aggregate_total_var(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, var_pop) //var_samp是样本方差，var_pop是总体方差
  }
  def aggregate_total_sum(collection: DataFrame, property: String): Number = {
    aggregate_template(collection, property, sum) //var_samp是样本方差，var_pop是总体方差
  }
  def aggregate_array(collection: DataFrame, property: String): List[Any] = {
    val propertyCol =
      try{
        collection.select(property)
      } catch {
        case _: Throwable => throw new IllegalArgumentException("The property does not exist.")}
    propertyCol.collect().map(_(0)).toList
  }
  def aggregate_count(collection: DataFrame, property: String): Int = {
    aggregate_template(collection, property, count).asInstanceOf[Int]
  }
  def aggregate_count_distinct(collection: DataFrame, property: String): Int = {
    val emptyString = Seq.empty[String]
    val countDistinct_ : (String=>Column) = value => countDistinct(value, emptyString:_*)
    aggregate_template(collection, property, countDistinct_).asInstanceOf[Int]
  }
  def aggregate_first(collection: DataFrame, property: String): Any = {
    if (! collection.columns.contains(property))
      throw new IllegalArgumentException("The property does not exist.")
    collection.first().getAs[Any](property)
  }

  def aggregate_stats(collection: DataFrame, property: String): mutable.Map[String, Number] = {
    // 1. 检查列是否存在
    if (!collection.columns.contains(property))
      throw new IllegalArgumentException(s"The property '$property' does not exist.")

    // 2. 检查数据类型是否为数值型
    val fieldType = collection.schema(property).dataType
    if (!fieldType.isInstanceOf[NumericType])
      throw new IllegalArgumentException(s"The property '$property' is not numeric.")

    // 3. 定义统计函数与名称
    val stats: List[String => Column] = List(
      functions.sum,
      functions.min,
      functions.max,
      functions.mean,
      functions.stddev_samp,
      functions.var_samp,
      functions.stddev_pop,
      functions.var_pop
    )

    val statNames: List[String] = List(
      "sum",
      "min",
      "max",
      "mean",
      "sample standard deviation",
      "sample variance",
      "total standard deviation",
      "total variance"
    )

    // 4. 聚合统计值
    val results = mutable.Map[String, Number]()
    for ((func, name) <- stats.zip(statNames)) {
      val resultRow = collection.agg(func(property).alias("value")).first()
      results(name) = resultRow.getAs[Number]("value")
    }
    results
  }

  def aggregate_bounds(collection: DataFrame, geometryFieldName: String = "geom"): OGEGeometry = {
    val srcSRID = collection.persist(StorageLevel.MEMORY_AND_DISK_SER).first().getAs[Geometry](geometryFieldName).getSRID
    if (!collection.columns.contains(geometryFieldName)) throw new IllegalArgumentException("The geometry field does not exist.")
    val geom = SedonaUtil.ST_envelopeAggregate(collection, geometryFieldName)
    collection.unpersist()
    geom.setSRID(srcSRID)
    new OGEGeometry(geom)
  }
  def describe(collection: DataFrame, property: List[String]): DataFrame = {
    // "count", "mean", "stddev", "min", "max"
    collection.describe(property:_*)
  }
  def propertyNames(collection: DataFrame): List[String] = {
    collection.columns.toList
  }

  def propertySet(collection: DataFrame, columnName: String, newContent: Any): DataFrame = {
    SedonaUtil.ST_Property_Set(collection, columnName, newContent)
  }

  def randomColumn(collection: DataFrame,
                   columnName: String = "random", seed: Long = 0,
                   distribution: String = "uniform", rowKeys: List[String] = List.empty): DataFrame = {
   //指定的属性（或属性组合）必须能唯一区分集合中的不同元素（类似数据库的主键）相同的 rowKeys + 相同的随机种子 → 生成相同的随机数序列
    // 验证分布类型
    if (!Seq("uniform", "normal").contains(distribution.toLowerCase)) {
      throw new IllegalArgumentException("Supported distributions: 'uniform' or 'normal'")
    }
    // 处理rowKeys（若提供，则基于rowKeys生成确定性随机数）
    val randomExpr = if (rowKeys.nonEmpty) {
      // 验证rowKeys是否存在于DataFrame中
      val missingKeys = rowKeys.diff(collection.columns)
      if (missingKeys.nonEmpty) {
        throw new IllegalArgumentException(s"Row keys not found: ${missingKeys.mkString(", ")}")
      }
      // 创建UDF：将行数据转换为确定性随机数
      val randomUDF = udf { (values: Seq[Any]) =>
        // 将所有值转换为字符串并拼接
        val combinedKey = values.mkString("_")
        // 使用MurmurHash3生成确定性哈希值
        val rowSeed = MurmurHash3.stringHash(combinedKey, seed.toInt).toLong

        // 创建基于种子的随机数生成器
        val rng = new java.util.Random(rowSeed)
        distribution.toLowerCase match {
          case "uniform" => rng.nextDouble()  // [0, 1)均匀分布
          case "normal" => rng.nextGaussian() // 正态分布(μ=0, σ=1)
        }
      }
      // 选择rowKeys列并应用UDF
      val rowKeyColumns = rowKeys.map(col)
      collection.withColumn(columnName, randomUDF(array(rowKeyColumns: _*)))
    } else {
      // 若未提供rowKeys，则使用全局种子
      distribution.toLowerCase match {
        case "uniform" => collection.withColumn(columnName, rand(seed))
        case "normal" => collection.withColumn(columnName, randn(seed))
      }
    }
    // 添加随机数列
    randomExpr
  }
  def select(collection: DataFrame, propertySelectors: List[String], newProperties: List[String] = null, retainGeometry: Boolean = true) = {
    // 默认几何列名为"geom"
    if (newProperties!= null && newProperties.length != propertySelectors.length) {
      throw new IllegalArgumentException("The length of propertySelectors and newProperties must be equal.")
    }
    val columns = collection.columns
    propertySelectors.foreach(s => if (!columns.contains(s)) throw new IllegalArgumentException("The property does not exist."))
    val collectionSelected =
      if (retainGeometry && columns.contains("geom")) collection.select("geom", propertySelectors: _*)
      else collection.select(propertySelectors.head, propertySelectors.tail: _*)
    if (newProperties==null) collectionSelected
    else {
      for (index <- propertySelectors.indices){
        collectionSelected.withColumnRenamed(propertySelectors(index), newProperties(index))
      }
      collectionSelected
    }
  }
  def distinct(collection: DataFrame, property: List[String] = null): DataFrame = {
    if (property == null) collection.distinct()
    else collection.dropDuplicates(property)
  }
  def remap(collection: DataFrame, lookupIn: List[Any], lookupOut: List[Any], property: String) = {
    if (lookupIn.length != lookupOut.length) {
      throw new IllegalArgumentException("The length of lookupIn and lookupOut must be equal.")
    }
    if (!collection.columns.contains(property)){
      throw new IllegalArgumentException(s"The property ${property} does not exist.")
    }
    val lookupMap = lookupIn.zip(lookupOut).toMap
    // 定义映射关系
//    val mapFunction: Any => Any = (in: Any) => {
//      lookupMap.get(in) match {
//        case Some(out) => out
//        case None => None
//      }
//    }
//    val mapFunctionUDF = udf(mapFunction)
//    collection.withColumn(property, mapFunctionUDF(collection.col(property)))
    collection.withColumn(property, when(collection.col(property).isin(lookupIn), lookupMap(col(property)))) //不符合条件的默认被映射为null
    collection.na.drop("any", Seq(property))
    collection
  }
  def size(collection: DataFrame): Long = {
    collection.count()
  }
  def sortAndSplit(collection: DataFrame, property: String, ascending: Boolean = true, geoHashPrecision: Int = 10): DataFrame = {
    // 默认几何列名为"geom"
    if (!collection.columns.contains(property)) throw new IllegalArgumentException("The property does not exist.")
    if (property == "geom") {
      // GeoHash分区
      collection.withColumn("geoHashValue", ST_GeoHash(collection.col(property), lit(geoHashPrecision)))
      if (ascending) collection.sort(collection.col(property).asc)
      else collection.sort(collection.col(property).desc).repartitionByRange()
    }
    else{
      // 范围(Range)分区，按照元素大小关系将其划分到不同分区，各分区内也进行排序，得到的结果全局有序
      if (ascending) collection.sort(collection.col(property).asc)
      else collection.sort(collection.col(property).desc)
      // 如果只需要分区内有序，使用sortWithinPartitions
    }
  }
  def extractRowsAsList(collection: DataFrame, count: Int, offset: Int = 0): DataFrame = {
    val dfWithRowNum = collection.withColumn(
      "row_num",
      row_number().over(Window.orderBy(lit(1))) // 使用 lit(1) 作为排序依据意味着所有行的排序顺序是相同的，因此行号的分配将基于行的物理顺序
    )
    // 提取第 m 行到第 n 行（包含两端）
    val result = dfWithRowNum
      .filter(col("row_num") >= offset + 1 && col("row_num") <= count + offset)
      .drop("row_num")
    result
  }

//  def aside(features: FeatureCollection, func, var_args): FeatureCollection ={
//
//  }
//  def copyProperties(destination: DataFrame, source: DataFrame, properties: List[String] = null, exclude: List[String] = null): DataFrame = {
//
//  }
//  def distance(features: FeatureCollection, searchRadius: Float = 100000, maxError: Float = 100): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//    //可以考虑加一个别名表示(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
//  }
  def map(collection: DataFrame, baseAlgorithm: String, dropNulls: Boolean): DataFrame = {
    collection
  }
//  def sort(collection: FeatureCollection, property: String, ascending: Boolean = true): FeatureCollection = {
//    //所以FeatureCollection类中应该有序号标识
//  }

  //基于DataFrame特性补充封装一些函数
  def sample(collection: DataFrame, fraction: Double, seed: Long = 0, withReplacement: Boolean = false): DataFrame = {
   // This is NOT guaranteed to provide exactly the fraction of the total count of the given
    // withReplacement: 采样过程中是否允许重复选择相同的行
    collection.sample(withReplacement, fraction, seed)
  }
  def randomSplit(collection: DataFrame, weights: Array[Double], seed: Long = 0): Array[DataFrame] = {
    collection.randomSplit(weights, seed)
  }

  def filterBounds(implicit sc: SparkContext,
                   collection: DataFrame, geometry: OGEGeometry,
                   geometryFieldName: String = "geom", useRDD: Boolean = false): DataFrame = {
    require(collection != null, "DataFrame cannot be null")
    if (useRDD) {
      // Using RDD-based approach
      val spark = SedonaUtil.sedonaSessionBuilder(sc)
      val spatialRDD = Adapter.toSpatialRdd(collection, geometryFieldName, collection.columns)
      val resultRDD = SedonaUtil.rangeQuery(spatialRDD, geometry, "Intersects")
      Adapter.toDf(resultRDD, collection.columns.filter(_ != geometryFieldName), spark)
        .withColumnRenamed("geometry", geometryFieldName)
    } else {
      // Using DataFrame-based approach
      val fields: Seq[StructField] = Seq(StructField("geom", GeometryUDT, nullable = true))
      val queryDF = collection.sparkSession.createDataFrame(List(Row.apply(geometry.geometry)).asJava, StructType(fields))
      SedonaUtil.rangeQueryDataFrame(collection, queryDF, geometryFieldName, "Intersects").withColumnRenamed("geometry", geometryFieldName)
    }
  }
  def filterDate(collection: DataFrame, start: String, end: String, timeFieldName: String = "time"): DataFrame = {
    collection.filter(row => {
      val date = row.getDate(collection.columns.indexOf(timeFieldName))
      date.after(Date.valueOf(start)) &&  date.before(Date.valueOf(end))
    })
  }
  def filterMetadata(collection: DataFrame, name: String, operator: String, value: String): DataFrame = {
    // 验证列名存在
    if (!collection.columns.contains(name)) {
      throw new IllegalArgumentException(s"列 '$name' 不存在")
    }
    // 获取列的数据类型
    val columnType = collection.schema(name).dataType
    // 根据运算符构建过滤条件
    // 根据数据类型转换值
    val convertedValue = columnType match {
      case _: StringType => value
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: DoubleType => value.toDouble
      case _: FloatType => value.toFloat
      case _: ShortType => value.toShort
      case _: ByteType => value.toByte
      case _: BooleanType => value.toBoolean
      case _: TimestampType => java.sql.Timestamp.valueOf(value)
      case _: DateType => java.sql.Date.valueOf(value)
      case _ => throw new IllegalArgumentException(s"不支持的列类型: ${columnType.simpleString}")
    }
    val condition: Column = operator.toLowerCase match {
      case "equals" =>
        col(name) === convertedValue
      case "less_than" =>
        col(name) < convertedValue
      case "greater_than" =>
        col(name) > convertedValue
      case "not_equals" =>
        col(name) =!= convertedValue
      case "not_less_than" =>
        col(name) >= convertedValue
      case "not_greater_than" =>
        col(name) <= convertedValue
      case "starts_with" | "ends_with" | "contains" | "not_starts_with" | "not_ends_with" | "not_contains" =>
        // 字符串操作要求列类型为StringType
        if (columnType != StringType) {
          throw new IllegalArgumentException(s"列 '$name' 不是字符串类型，无法使用字符串比较运算符")
        }
        operator.toLowerCase match {
          case "starts_with" => col(name).startsWith(value)
          case "ends_with" => col(name).endsWith(value)
          case "contains" => col(name).contains(value)
          case "not_starts_with" => !col(name).startsWith(value)
          case "not_ends_with" => !col(name).endsWith(value)
          case "not_contains" => !col(name).contains(value)
        }
      case _ =>
        throw new IllegalArgumentException(s"不支持的运算符: '$operator'")
    }
    // 应用过滤条件
    collection.filter(condition).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def spatialPredicateQuery(collection: DataFrame, geometry: OGEGeometry, geometryFieldName: String = "geom",
                            spatialPredicate: String = "Intersects", properties: String = "geometry"): DataFrame = {
    //返回collection中满足查询谓词关系的部分
    require(collection != null, "DataFrame cannot be null")
    val fields: Seq[StructField] = Seq(StructField("geom", GeometryUDT, nullable = true))
    val queryDF = collection.sparkSession.createDataFrame(List(Row.apply(geometry.geometry)).asJava, StructType(fields))
    val result = SedonaUtil.rangeQueryDataFrame(collection, queryDF, geometryFieldName, spatialPredicate, properties)
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //返回collection与geometry的交集部分
//    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
//    val spatialRDD = Adapter.toSpatialRdd(collection, geometryFieldName, collection.columns)
//    val IntersectRDD = SedonaUtil.rangeQuery(spatialRDD, geometry, spatialPredicate)
//    val resultRDD = new SpatialRDD[Geometry]()
//    val srcSRID = spatialRDD.rawSpatialRDD.first().getSRID
//    resultRDD.setRawSpatialRDD(IntersectRDD.rawSpatialRDD.map(g => {
//      val intersect = g.intersection(geometry.geometry)
//      intersect.setSRID(srcSRID)
//      intersect
//    }))
//    Adapter.toDf(resultRDD, collection.columns.filter(_!=geometryFieldName), spark).withColumn(geometryFieldName, col("geometry")).drop("geometry")
  }

  def intersection(implicit sc: SparkContext, collection1: DataFrame, collection2: DataFrame, geometryFieldName1: String = "geom", geometryFieldName2: String = "geom"): DataFrame = {
    //所有图层和/或要素类中相叠置的要素或要素的各部分将被写入输出要素类
    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    val spatialRDD1 = Adapter.toSpatialRdd(collection1, geometryFieldName1, collection1.columns)
    val spatialRDD2 = Adapter.toSpatialRdd(collection2, geometryFieldName2, collection2.columns)
    spatialRDD1.analyze()
    spatialRDD2.analyze()
    if (spatialRDD1.approximateTotalCount == 0 || spatialRDD2.approximateTotalCount == 0) return spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], collection1.schema)
    val javaPairRDD = SedonaUtil.spatialJoinQueryFlat(sc, spatialRDD2, spatialRDD1, true, "Intersects").cache()
    //    val userDataStructType = StructType(collection1.schema.filter(f=>f.name!=geometryFieldName1))
    val srcSRID: Int = spatialRDD1.rawSpatialRDD.first().getSRID
    val javaRDD: JavaRDD[Geometry] = javaPairRDD.map(geomPair => {
      val intersectionGeom = geomPair._1.intersection(geomPair._1).intersection(geomPair._2.intersection(geomPair._2))
      intersectionGeom.setUserData(geomPair._1.getUserData)
      intersectionGeom.setSRID(srcSRID)
      intersectionGeom
    })
    val resultRDD = new SpatialRDD[Geometry]()
    resultRDD.setRawSpatialRDD(javaRDD)
    Adapter.toDf(resultRDD, collection1.columns.filter(_ != geometryFieldName1), spark).withColumnRenamed("geometry", geometryFieldName1).cache()
  }

  def difference(implicit sc: SparkContext, collection1: DataFrame, collection2: DataFrame,
                 geometryFieldName1: String = "geom", geometryFieldName2: String = "geom"): DataFrame = {
    SedonaUtil.ST_Difference(collection1,collection2,geometryFieldName1,geometryFieldName2).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def union(implicit sc: SparkContext, collection1: DataFrame, collection2: DataFrame,
                 geometryFieldName: String = "geom"): DataFrame = {
    val collectionList = List (collection1,collection2)
    val colUnion = mergeAll(collectionList)
    aggregate_union(colUnion,geometryFieldName).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }
  def erase(implicit sc: SparkContext, input: DataFrame, erase: DataFrame, geometryFieldName1: String = "geom", geometryFieldName2: String = "geom"): DataFrame = {
    //只将输入要素处于擦除要素外部边界之外的部分复制到输出要素
    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    val inputRDD = Adapter.toSpatialRdd(input, geometryFieldName1, input.columns)
    val eraseRDD = Adapter.toSpatialRdd(erase, geometryFieldName2, erase.columns)
    inputRDD.analyze()
    eraseRDD.analyze()
    if (inputRDD.approximateTotalCount == 0) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], input.schema)
    }
    if (eraseRDD.approximateTotalCount == 0) {
      return input
    }
    val srcSRID: Int = inputRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_AND_DISK_SER).first().getSRID
    inputRDD.rawSpatialRDD.map(geom => geom.intersection(geom)) //解决多边形存在自相交的情况
    eraseRDD.rawSpatialRDD.map(geom => geom.intersection(geom))
    val intersectJavaPairRDD: JavaPairRDD[Geometry, util.List[Geometry]] = SedonaUtil.spatialJoinQuery(sc, eraseRDD, inputRDD, true, "Intersects")
    val overlapJavaPairRDD: JavaPairRDD[Geometry, util.List[Geometry]] = SedonaUtil.spatialJoinQuery(sc, eraseRDD, inputRDD, true, "Overlaps")

    val resultRDD1: JavaRDD[Geometry] = overlapJavaPairRDD.toJavaRDD().map { case (inputGeom, eraseGeoms) =>
      // 合并所有擦除要素为一个几何
      val mergedEraseGeom = eraseGeoms.asScala.foldLeft(eraseGeoms.get(0)) { (current, geom) =>
        current.intersection(current).union(geom.intersection(geom))
      }
      // 计算输入要素与合并后擦除要素的差集
      val erasedGeom = inputGeom.intersection(inputGeom).difference(mergedEraseGeom)
      erasedGeom.setUserData(inputGeom.getUserData)
      erasedGeom
    }
    val resultRDD2: JavaRDD[Geometry] = inputRDD.rawSpatialRDD.subtract(intersectJavaPairRDD.map(i=>i._1).toJavaRDD()) //相离的要素
    inputRDD.rawSpatialRDD.unpersist()
    val resultSpatialRDD = new SpatialRDD[Geometry]()
    resultSpatialRDD.setRawSpatialRDD(resultRDD1.union(resultRDD2).map(g => {
      g.setSRID(srcSRID)
      g
    }))
    Adapter.toDf(resultSpatialRDD, input.columns.filter(_!=geometryFieldName1), spark).withColumnRenamed("geometry", geometryFieldName1).persist(StorageLevel.MEMORY_AND_DISK_SER)//.withColumn(geometryFieldName1, col("geometry")).drop("geometry") //input.columns.filter(_!=geometryFieldName1)这里很重要，否则会多一个字段！生成的几何列默认命名是“geometry”
  }

  def differenceWithGeometry(implicit sc: SparkContext, collection: DataFrame, geometry: OGEGeometry, geometryFieldName: String = "geom"): DataFrame = {
    //返回collection与geometry的差集部分（collection中不属于geometry的部分）
    //要素 A：与 Geometry 相交 → 结果包含 A 与 Geometry 的差集部分。
    //要素 B：完全在 Geometry 内部 → 结果包含空几何（或被过滤，取决于 difference 方法的行为）。
    //要素 C：与 Geometry 相离 → 结果直接包含完整的要素 C。
    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    val spatialRDD = Adapter.toSpatialRdd(collection, geometryFieldName, collection.columns)
    // 1. 找出所有与geometry相交的要素
    val intersectRDD = SedonaUtil.rangeQuery(spatialRDD, geometry, "Intersects")
    // 2. 计算相交要素的差集（geometry之外的部分）
    val differenceRDD = new SpatialRDD[Geometry]()
    differenceRDD.setRawSpatialRDD(intersectRDD.rawSpatialRDD.map(g => {
      g.difference(geometry.geometry) // 计算差集
    }))
    // 3. 找出所有与geometry相离的要素（不相交的）
    val disjointRDD = new SpatialRDD[Geometry]()
    disjointRDD.setRawSpatialRDD(spatialRDD.rawSpatialRDD.subtract(intersectRDD.rawSpatialRDD))
    // 4. 合并差集结果和相离要素
    val resultRDD = new SpatialRDD[Geometry]()
    val srcSRID = spatialRDD.rawSpatialRDD.first().getSRID
    resultRDD.setRawSpatialRDD(differenceRDD.rawSpatialRDD.union(disjointRDD.rawSpatialRDD).map(g => {
      g.setSRID(srcSRID)
      g
    }))
    // 5. 转换回DataFrame
    Adapter.toDf(resultRDD, collection.columns.filter(_!=geometryFieldName), spark).withColumnRenamed("geometry", geometryFieldName)
  }

  //这是一个临时性函数，后面最好通过map实现
  //增加字段”area、length、perimeter、boundary“为每个矢量计算面积（多边形）、长度（线性）、周长
  def area(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_Area(collection, geometryFieldName)
  }

  def length(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_Length(collection, geometryFieldName)
  }

  def perimeter(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_Perimeter(collection, geometryFieldName)
  }

  def boundary(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_Boundary(collection, geometryFieldName)
  }
  def buffer(implicit sc: SparkContext, collection: DataFrame, distance: Double, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_Buffer(collection,geometryFieldName,distance)
  }

  def geoHash(collection: DataFrame, geometryFieldName: String = "geom", geoHashPrecision: Int = 10): DataFrame = {
    SedonaUtil.ST_GeoHash(collection, geometryFieldName, geoHashPrecision)
  }

  def delaunay(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_DelaunayTriangles(collection, geometryFieldName)
  }

  def voronoi(collection: DataFrame, geometryFieldName: String = "geom"): DataFrame = {
    SedonaUtil.ST_VoronoiPolygons(collection, geometryFieldName)
  }

  def constantColumn(collection: DataFrame, constantValue: Any, columnName: String = "constant"): DataFrame = {
    collection.withColumn(columnName, lit(constantValue))
  }
  def reproject(implicit sc: SparkContext, collection: DataFrame, tarCrsCode: String = "EPSG:4326", geometryFieldName: String = "geom"): DataFrame = {
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    val spatialRDD = Adapter.toSpatialRdd(collection, geometryFieldName, collection.columns)
    val srcCrsCode = "EPSG:"+spatialRDD.rawSpatialRDD.first().getSRID.toString
    spatialRDD.CRSTransform(srcCrsCode, tarCrsCode) //虽然坐标数字能转换，但这一步转换后，内部Geometry对象SRID属性会被置为0，需要再次为其设置正确的SRID
    //RDD具有不可变性，返回一个新值
    val resultRDD = new SpatialRDD[Geometry]()
    resultRDD.setRawSpatialRDD(spatialRDD.rawSpatialRDD.map(g => {
      g.setSRID(OGEGeometry.crsToSRID(tarCrsCode))
      g
    }))
    Adapter.toDf(resultRDD, collection.columns.filter(_!=geometryFieldName), spark).withColumnRenamed("geometry", geometryFieldName).cache()
  }
  def subtract(collection: DataFrame, column1: String, column2: String, newColumnName: String): DataFrame = {
    // 验证列是否存在
    if (!collection.columns.contains(column1)) {
      throw new IllegalArgumentException(s"列 '$column1' 不存在")
    }
    if (!collection.columns.contains(column2)) {
      throw new IllegalArgumentException(s"列 '$column2' 不存在")
    }
    // 获取列的数据类型
    val schema: StructType = collection.schema
    val column1Type = schema(column1).dataType
    val column2Type = schema(column2).dataType
    // 验证两列是否均为数值类型
    if (!column1Type.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(s"列 '$column1' 不是数值类型")
    }
    if (!column2Type.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(s"列 '$column2' 不是数值类型")
    }
    // 执行相减操作
    collection.withColumn(newColumnName, col(column1) - col(column2))
  }

  def combine(collection: DataFrame, column1: String, column2: String, newColumnName: String): DataFrame = {
    // 验证列是否存在
    if (!collection.columns.contains(column1)) {
      throw new IllegalArgumentException(s"列 '$column1' 不存在")
    }
    if (!collection.columns.contains(column2)) {
      throw new IllegalArgumentException(s"列 '$column2' 不存在")
    }
    // 获取列的数据类型
    val schema: StructType = collection.schema
    val column1Type = schema(column1).dataType
    val column2Type = schema(column2).dataType
    // 验证两列是否均为数值类型
    if (!column1Type.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(s"列 '$column1' 不是数值类型")
    }
    if (!column2Type.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(s"列 '$column2' 不是数值类型")
    }
    // 执行相加操作
    collection.withColumn(newColumnName, col(column1) + col(column2))
  }

  def limit(collection: DataFrame, max: Int, property: String = null, ascending: Boolean = true): DataFrame = {
    val sortedCollection: DataFrame =
      if (property==null) collection
      else sortAndSplit(collection, property, ascending)
    sortedCollection.limit(max)
  }

  def assembleNumericArray(collection: DataFrame, properties: List[String], name: String = "array"): DataFrame = {
    // 1. 验证所有properties列都存在于DataFrame中
    val missingCols = properties.diff(collection.columns)
    if (missingCols.nonEmpty) {
      throw new IllegalArgumentException(s"Properties中包含不存在的列: ${missingCols.mkString(", ")}")
    }
    // 2. 验证所有properties列都是数值类型
    val nonNumericCols = properties.filter { colName =>
      val colType = collection.schema(colName).dataType
      !Seq("ByteType", "ShortType", "IntegerType", "LongType", "FloatType", "DoubleType").contains(colType.typeName)
    }
    if (nonNumericCols.nonEmpty) {
      throw new IllegalArgumentException(s"Properties中包含非数值类型的列: ${nonNumericCols.mkString(", ")}")
    }
    // 3. 过滤缺失值并创建数组列
    collection
      .filter(properties.map(c => col(c).isNotNull).reduce(_ && _))  // 过滤任何列有NULL的行
      .select(
        col("*"),  // 保留原所有列
        array(properties.map(col): _*).alias(name)  // 创建数值数组列
      )
  }

  //连接查询（空间连接，距离连接）
  //空间连接（spatialJoin）
  def spatialJoinOneToOne(implicit sc: SparkContext, leftCollection: DataFrame, rightCollection: DataFrame, reduceProperties: List[String], reduceRules: List[String],
                          leftGeometryFieldName: String = "geom", rightGeometryFieldName: String = "geom", useIndex: Boolean = true, spatialPredicate: String = "Contains"): DataFrame = {
    val spatialJoinDF = SedonaUtil.spatialJoinOneToOne(sc, leftCollection, rightCollection, leftGeometryFieldName, rightGeometryFieldName, useIndex, spatialPredicate, reduceProperties, reduceRules)
    spatialJoinDF//.withColumn(leftGeometryFieldName, col("geometry")).drop("geometry")
  }


  /**
   * 完全 Spark SQL 的空间连接查询实现
   *
   * @param leftSpatialDataFrame geometry object dataFrame
   * @param rightSpatialDataFrame query object dataFrame
   * @param leftGeometryFieldName  geometry field name
   * @param rightGeometryFieldName geometry field name
   * @param spatialPredicate predicate(Valid options are: Contains, Crosses, Equals, Intersects, Overlaps, Touches, Within, Covers, CoveredBy)
   * @return 要素-要素的一对一映射
   */
  def spatialJoinQueryFlat(implicit sc: SparkContext,
                           leftSpatialDataFrame: DataFrame,
                           rightSpatialDataFrame: DataFrame,
                           leftGeometryFieldName: String = "geom",
                           rightGeometryFieldName: String = "geom",
                           spatialPredicate: String = "Contains",
                           useRDD: Boolean = true,
                           useIndex: Boolean = true,
                           gridType: String = "QUADTREE"): DataFrame = {
    if (useRDD) {
      // 使用 RDD 的空间连接查询
      val leftSpatialRDD = Adapter.toSpatialRdd(leftSpatialDataFrame, leftGeometryFieldName, leftSpatialDataFrame.columns)
      val rightSpatialRDD = Adapter.toSpatialRdd(rightSpatialDataFrame, rightGeometryFieldName, rightSpatialDataFrame.columns)
      leftSpatialRDD.analyze()
      rightSpatialRDD.analyze()
      val rdd = SedonaUtil
        .spatialJoinQueryFlat(sc, leftSpatialRDD, rightSpatialRDD, useIndex, spatialPredicate, gridType)
      Adapter.toDf(rdd, leftSpatialDataFrame.sparkSession)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      // 使用 DataFrame 的空间连接查询
      SedonaUtil
        .spatialJoinQueryFlatDataFrame(
          leftSpatialDataFrame,
          rightSpatialDataFrame,
          leftGeometryFieldName,
          rightGeometryFieldName,
          spatialPredicate
        )
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }

  /**
   * 完全 Spark SQL 的空间连接查询实现
   *
   * @param leftSpatialDataFrame   geometry object dataFrame
   * @param rightSpatialDataFrame  query object dataFrame
   * @param leftGeometryFieldName  geometry field name
   * @param rightGeometryFieldName geometry field name
   * @param spatialPredicate       predicate(Valid options are: Contains, Crosses, Equals, Intersects, Overlaps, Touches, Within, Covers, CoveredBy)
   * @return 要素-!!要素集合!!的一对多映射
   */
  def spatialJoinQuery(implicit sc: SparkContext,
                       leftSpatialDataFrame: DataFrame,
                       rightSpatialDataFrame: DataFrame,
                       leftGeometryFieldName: String = "geom",
                       rightGeometryFieldName: String = "geom",
                       spatialPredicate: String = "Contains",
                       useRDD: Boolean = true,
                       useIndex: Boolean = true,
                       gridType: String = "QUADTREE"): DataFrame = {

    if (useRDD) {
      // 使用 RDD 的空间连接查询
      val leftSpatialRDD = Adapter.toSpatialRdd(leftSpatialDataFrame, leftGeometryFieldName, leftSpatialDataFrame.columns)
      val rightSpatialRDD = Adapter.toSpatialRdd(rightSpatialDataFrame, rightGeometryFieldName, rightSpatialDataFrame.columns)
      leftSpatialRDD.analyze()
      rightSpatialRDD.analyze()
      val resultRDD = SedonaUtil
        .spatialJoinQuery(sc, leftSpatialRDD, rightSpatialRDD, useIndex, spatialPredicate, gridType)
      val rdd = resultRDD.rdd.map { case (geom, geomList) =>
        Row(geom, geomList.asScala)
      }
      val schema = StructType(Seq(
        StructField("query_geom", GeometryUDT, nullable = false),
        StructField("matched_list", ArrayType(GeometryUDT), nullable = false)
      ))
      leftSpatialDataFrame.sparkSession
        .createDataFrame(rdd, schema)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      // 使用 DataFrame 的空间连接查询
      SedonaUtil
        .spatialJoinQueryDataFrame(
          leftSpatialDataFrame,
          rightSpatialDataFrame,
          leftGeometryFieldName,
          rightGeometryFieldName,
          spatialPredicate
        )
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }

  //距离连接（distanceJoin）
  def distanceJoinQueryFlat(implicit sc: SparkContext, spatialDataFrame: DataFrame, queryDataFrame: DataFrame,
                            spatialGeometryFieldName: String = "geom", queryGeometryFieldName: String = "geom",
                            distance: Double = 0.0, limit: String = "less_than",
                            useRDD: Boolean = true,
                            useIndex: Boolean = true, spatialPredicate: String = "Contains", gridType: String = "QUADTREE"): DataFrame = {

    if (useRDD) {
      // 使用 RDD 模式进行距离连接查询
      val spatialRDD = Adapter.toSpatialRdd(spatialDataFrame, spatialGeometryFieldName, spatialDataFrame.columns)
      val queryRDD = Adapter.toSpatialRdd(queryDataFrame, queryGeometryFieldName, queryDataFrame.columns)
      spatialRDD.analyze()
      queryRDD.analyze()
      SedonaUtil
        .distanceJoinQueryFlat(sc, spatialRDD, queryRDD, distance, useIndex, spatialPredicate, gridType)
        .withColumnRenamed("_1", "query_geom")
        .withColumnRenamed("_2", "matched_geom")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      // 使用 DataFrame 模式进行距离连接查询
      SedonaUtil
        .distanceJoinQueryFlatDataFrame(
          spatialDataFrame,
          queryDataFrame,
          spatialGeometryFieldName,
          queryGeometryFieldName,
          distance,
          limit
        )
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }


  def distanceJoinQuery(implicit sc: SparkContext,
                        spatialDataFrame: DataFrame,
                        queryDataFrame: DataFrame,
                        spatialGeometryFieldName: String = "geom",
                        queryGeometryFieldName: String = "geom",
                        distance: Double = 0.0,
                        limit: String = "less_than",
                        useRDD: Boolean = true,
                        useIndex: Boolean = true,
                        spatialPredicate: String = "Contains",
                        gridType: String = "QUADTREE"): DataFrame = {

    if (useRDD) {
      // 使用 RDD 方式进行距离连接
      val spatialRDD = Adapter.toSpatialRdd(spatialDataFrame, spatialGeometryFieldName, spatialDataFrame.columns)
      val queryRDD = Adapter.toSpatialRdd(queryDataFrame, queryGeometryFieldName, queryDataFrame.columns)
      spatialRDD.analyze()
      queryRDD.analyze()
      SedonaUtil
        .distanceJoinQuery(sc, spatialRDD, queryRDD, distance, useIndex, spatialPredicate, gridType)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      // 使用 DataFrame 方式进行距离连接
      SedonaUtil
        .distanceJoinQueryDataFrame(
          spatialDataFrame,
          queryDataFrame,
          spatialGeometryFieldName,
          queryGeometryFieldName,
          distance,
          limit
        )
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }


  //k-近邻查询（k-NN）
  def kNNQuery(implicit sc: SparkContext,
               spatialDataFrame: DataFrame,
               queryGeometry: OGEGeometry,
               k: Int,
               geometryFieldName: String = "geom",
               useRDD: Boolean = false,
               useIndex: Boolean = true,
               spatialPredicate: String = "Contains",
               gridType: String = "QUADTREE"): DataFrame = {

    if (useRDD) {
      // 使用 RDD 模式进行 K 最近邻查询
      val spatialRDD = Adapter.toSpatialRdd(spatialDataFrame, geometryFieldName, spatialDataFrame.columns)
      spatialRDD.analyze()
      SedonaUtil
        .kNNQuery(sc, spatialRDD, queryGeometry, k, useIndex, spatialPredicate, gridType)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      // 使用 DataFrame 模式进行 K 最近邻查询
      SedonaUtil
        .kNNQueryDataFrame(spatialDataFrame, queryGeometry, k, geometryFieldName)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }
  //计算端原Feature函数接口实现
  //简单克里金插值
  def simpleKriging(implicit sc: SparkContext,
                    collection: DataFrame, propertyName: String,
                    cols: Int = 256,
                    rows: Int = 256,
                    defaultValue: Double = 100.0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val spatialRdd: SpatialRDD[Geometry] = Adapter.toSpatialRdd(collection, "geom", collection.columns)
    spatialRdd.analyze()
    val envelope = spatialRdd.boundaryEnvelope
    val extent = Extent(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val rasterExtent = RasterExtent(extent, cols, rows)
    val points: Array[PointFeature[Double]] = spatialRdd.rawSpatialRDD.map(t => {
      val p = vector.Point(t.getCoordinate)
      //直接转换为Double类型会报错 var data = t._2._2(propertyName).asInstanceOf[Double]
      val list = t.getUserData.asInstanceOf[List[String]]
      var data: Double = list(collection.columns.indexOf(propertyName)).toDouble
      if (data < 0) {
        data = defaultValue
      }
      PointFeature(p, data)
    }).collect().asScala.toArray
    println()
    val sv: Semivariogram = NonLinearSemivariogram(points, 30000, 0, Spherical)
    val method = new SimpleKrigingMethods {

      override def self: Traversable[PointFeature[Double]] = points
    }
    val originCoverage = method.simpleKriging(rasterExtent, sv)

    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(spatialRdd.rawSpatialRDD.first().getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = originCoverage.cellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    originCoverage.toArrayTile()
    var list: List[Tile] = List.empty
    list = list :+ originCoverage
    val tileRDD = sc.parallelize(list)
    val imageRDD = tileRDD.map(t => {
      val k = cn.entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val v = MultibandTile(t)
      (k, v)
    })

    (imageRDD, tileLayerMetadata)
  }
  //反距离加权权重
  def interpolateByIDW(
                        implicit sc: SparkContext,
                        collection: DataFrame,
                        propertyName: String,
                        maskGeom: OGEGeometry,
                        cols: Int = 256,
                        rows: Int = 256,
                        defaultValue: Double = 100.0
                      ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    // 1.转为 sedona SpatialRDD
    val spatialRdd: SpatialRDD[Geometry] = Adapter.toSpatialRdd(collection, "geom", collection.columns)
    spatialRdd.analyze()
    val envelope = spatialRdd.boundaryEnvelope
    val extent = Extent(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val rasterExtent = RasterExtent(extent, cols, rows)

    // 2.构建 PointFeature[Double] 数组
    val points: Array[PointFeature[Double]] = spatialRdd.rawSpatialRDD.map { geom =>
      val list = geom.getUserData.asInstanceOf[List[String]]
      val valueStr = list(collection.columns.indexOf(propertyName))
      val value = try {
        val v = valueStr.toDouble
        if (v < 0) defaultValue else v
      } catch {
        case _: Throwable => defaultValue
      }
      val point = geom.asInstanceOf[Point]
      PointFeature(point, value)
    }.collect().asScala.toArray

    // 3.进行 IDW 插值并裁剪
    val interpolatedRaster = InverseDistanceWeighted(points, rasterExtent)
    val maskPolygon = maskGeom.geometry
    val maskedRaster = interpolatedRaster.mask(maskPolygon)

    // 4.构建图层元数据
    val layout = TileLayout(1, 1, cols, rows)
    val layoutDef = LayoutDefinition(extent, layout)
    val crs = CRS.fromEpsgCode(spatialRdd.rawSpatialRDD.first().getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = maskedRaster.tile.cellType
    val tileLayerMetadata = TileLayerMetadata(cellType, layoutDef, extent, crs, bounds)

    // 5.构造输出 RDD
    val imageRDD = sc.parallelize(Seq(maskedRaster.tile)).map { tile =>
      val key = SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val multiband = MultibandTile(tile)
      (key, multiband)
    }

    (imageRDD, tileLayerMetadata)
  }

  def rasterize(collection: DataFrame, propertyName: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val spatialRdd: SpatialRDD[Geometry] = Adapter.toSpatialRdd(collection, "geom", collection.columns)
    spatialRdd.analyze()
    val envelope = spatialRdd.boundaryEnvelope
    val extent = Extent(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val tl = TileLayout(1, 1, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(spatialRdd.rawSpatialRDD.first().getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    val featureRDDforRaster: RDD[vector.Feature[Geometry, Double]] = spatialRdd.rawSpatialRDD.map(t => {
      //val data = t._2._2(propertyName).asInstanceOf[Double]
      val list = t.getUserData.asInstanceOf[List[String]]
      val data = list(collection.columns.indexOf(propertyName)).toDouble
      val feature = new vector.Feature[Geometry, Double](t, data)
      feature
    })
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = cn.entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })
    (imageRDD, tileLayerMetadata)
  }


  import com.alibaba.fastjson.serializer.JSONSerializer
  import java.io.IOException


  /**
    * Returns a GeoJSON string representation of the geometry.
    *
    * @param featureRDD the featureRDD to operate
    * @return
    */
  def toGeoJSONString(collection: DataFrame): String = {
    //    collection.repartition(1).write.format("geojson").mode("overwrite").save("C:\\Users\\HUAWEI\\Desktop\\vector_test")
    val columnNames: Array[String] = collection.columns.filterNot(_ == "geom")
    val jsonObject = new JSONObject
    val geoArray = new JSONArray()
    collection.collect().foreach(row => {
      val combinedObject = new JSONObject()
      val geojsonString: String = OGEGeometry.toGeoJSONString(row.getAs[Geometry]("geom")) //GeometryUDT.deserialize(row.getAs[GeometryUDT]("geom"))
      val coors = JSON.parseObject(geojsonString)
      val pro = new JSONObject()
      for (field <- columnNames) {
        val value = row.get(row.fieldIndex(field))
        if (value == null) pro.put(field, "")
        else if (value.isInstanceOf[Geometry]) {
          val writer = new WKTWriter()
          pro.put(field, writer.write(value.asInstanceOf[Geometry]))
        }
        else pro.put(field, value)
      }
      combinedObject.put("type", "Feature")
      combinedObject.put("geometry", coors)
      combinedObject.put("properties", pro)
      geoArray.add(combinedObject)
      ()
    })
    jsonObject.put("type", "FeatureCollection")
    jsonObject.put("features", geoArray)
    import com.alibaba.fastjson.parser.ParserConfig
    //    import whu.edu.cn.util.FeatureCollectionUtil.{GeometrySerializer => GS}
    //    ParserConfig.getGlobalInstance.putDeserializer(classOf[Geometry], new GeometrySerializer().asInstanceOf[ObjectSerializer])
    //    ParserConfig.global.createJavaBeanDeserializer(classOf[Geometry], new GeometrySerializer())
    //    ParserConfig.getGlobalInstance.ser
    //    val geoJSONString: String = JSON.toJSONString(jsonObject, SerializerFeature.IgnoreNonFieldGetter)
    val geoJSONString: String = jsonObject.toJSONString()

    geoJSONString
  }


  def saveJSONToServer(geoJSONString: String): String = {
    val time = System.currentTimeMillis()
    val host = GlobalConfig.QGISConf.QGIS_HOST
    val userName = GlobalConfig.QGISConf.QGIS_USERNAME
    val password = GlobalConfig.QGISConf.QGIS_PASSWORD
    val port = GlobalConfig.QGISConf.QGIS_PORT
    val outputVectorPath = s"${GlobalConfig.Others.jsonSavePath}vector_${time}.json"
    // 创建PrintWriter对象
    val writer: BufferedWriter = new BufferedWriter(new FileWriter(outputVectorPath))
    // 写入JSON字符串
    writer.write(geoJSONString)
    // 关闭PrintWriter
    writer.close()
    versouSshUtil(host, userName, password, port)
    val st = s"scp  $outputVectorPath root@${GlobalConfig.Others.tomcatHost}:/home/oge/tomcat/apache-tomcat-8.5.57/webapps/oge_vector/vector_${time}.json"

    //本地测试使用代码
    //      val exitCode: Int = st.!
    //      if (exitCode == 0) {
    //        println("SCP command executed successfully.")
    //      } else {
    //        println(s"SCP command failed with exit code $exitCode.")
    //      }
    runCmd(st, "UTF-8")
    println(s"st = $st")
    val storageURL = s"${GlobalConfig.Others.tomcatHost_public}/tomcat-vector/vector_" + time + ".json"
    storageURL
  }

  def visualize(featureCollection: DataFrame, color:List[String], attribute:String): Unit = {
    val geoJson = new JSONObject
    val render = new JSONObject
    val geoJSONString: String = toGeoJSONString(featureCollection)
    val url = saveJSONToServer(geoJSONString)
    geoJson.put(Trigger.layerName, url)
    val colorArray = new JSONArray()
    for (st <- color) {
      colorArray.add(st)
    }
    render.put("color", colorArray)
    render.put("attribute", attribute)
    geoJson.put("render", render)
    PostSender.shelvePost("vector", geoJson)
  }

  def exportAndUploadFeatures(implicit sc: SparkContext, featureCollection: DataFrame, batchParam: BatchParam, dagId: String): Unit = {
    val spark = SedonaUtil.sedonaSessionBuilder(sc)
    // 上传文件
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val obj: JSONObject = new JSONObject
    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
    obj.put("path", path.toString)
    PostSender.shelvePost("info", obj)
    val hadoopConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}"
    batchParam.getFormat.toLowerCase match {
      case "csv" => {
        val path = batchParam.getUserId + "/result/" + batchParam.getFileName + ".csv"
        featureCollection.withColumn("geom", col("geom").cast("string"))
//          .withColumn("geometry", expr("unbase64(geom_str)"))
//          .repartition(3) //写入单个文件
          .repartition(1)
          .write
          .option("header", "true")
          .option("inferSchema", "true")
//          .option("quote", "\"") // 用引号包裹字段
//          .option("escape", "\"") // 转义引号
//          .option("fieldDelimiter", ",") // 明确字段分隔符
//          .option("encoding", "UTF-8") //编码类型，默认就是UTF-8
          .mode("overwrite")
          .csv(saveFilePath)
        val srcPath = new Path(saveFilePath)
        val destPath = new Path(saveFilePath + ".csv")
        FileUtil.copyMerge(fs, srcPath, fs, destPath, false, hadoopConf, null)
        clientUtil.Upload(path, saveFilePath + ".csv")
      }
      case "geojson" => {
        val path = batchParam.getUserId + "/result/" + batchParam.getFileName + ".geojson"
        val destPath = saveFilePath + ".geojson"
        val writer: BufferedWriter = new BufferedWriter(new FileWriter(destPath)) // 创建PrintWriter对象
        writer.write(toGeoJSONString(featureCollection)) // 写入GeoJSON字符串
        writer.close() // 关闭PrintWriter
        clientUtil.Upload(path, destPath)
      }
      case "geoparquet" => {
        val path = batchParam.getUserId + "/result/" + batchParam.getFileName + ".geoparquet"
        val srcPath = new Path(saveFilePath)
        val destPath = new Path(saveFilePath + ".parquet")
        featureCollection.withColumn("geom", col("geom").cast("string"))
//          .withColumn("geometry", expr("unbase64(geom_str)"))
          .write.format("geoparquet")
          .mode("overwrite")
          .save(saveFilePath)
        FileUtil.copyMerge(fs, srcPath, fs, destPath, true, hadoopConf, null)
        clientUtil.Upload(path, saveFilePath + ".parquet")
      }
      case "shp" =>{
        val pathWithoutSuffix = batchParam.getUserId + "/result/" + batchParam.getFileName
        val destPath = saveFilePath + ".geojson"
        val writer: BufferedWriter = new BufferedWriter(new FileWriter(destPath)) // 创建PrintWriter对象
        writer.write(toGeoJSONString(featureCollection)) // 写入GeoJSON字符串
        writer.close() // 关闭PrintWriter
        val geojsonFile = new File(destPath)
        FeatureCollectionUtil.geoJSON2Shp(geojsonFile, saveFilePath+".shp")
        clientUtil.Upload(pathWithoutSuffix+".shp", saveFilePath + ".shp")
        clientUtil.Upload(pathWithoutSuffix+".dbf", saveFilePath + ".dbf")
        clientUtil.Upload(pathWithoutSuffix+".fix", saveFilePath + ".fix")
        clientUtil.Upload(pathWithoutSuffix+".prj", saveFilePath + ".prj")
        clientUtil.Upload(pathWithoutSuffix+".shx", saveFilePath + ".shx")
        //.cpg 编码配置文件
      }
    }
  }

  def cultivatedLandSuitability(sc: SparkContext, landTable: String, ecologyTable: String, urbanTable: String, slopeTable: String): DataFrame = {
    //需要确认：SparkSession的配置是否能在SparkContext基础上更新
    val sedona = SedonaUtil.sedonaSessionBuilder(sc)
    val query1 = s"SELECT geom, DLMC, ZLDWMC FROM ${landTable} WHERE DLMC IN ('旱地', '水浇地', '水田') AND st_isvalid(geom) = 't' "
    val query2 = s"SELECT geom, pdjb FROM ${slopeTable} where st_isvalid(geom) = 't' and pdjb > '4'"
    val cultivated = loadWithRestrictions(sc, query1, "geom")
    val slope = loadWithRestrictions(sc, query2, "geom")
    val urban = load(sc, s"${urbanTable}")
    val ecology = load(sc, s"${ecologyTable}")

    val slopeRDD = Adapter.toSpatialRdd(slope, "geom")
    val cultivatedRDD = Adapter.toSpatialRdd(cultivated, "geom")
    val ecologyRDD = Adapter.toSpatialRdd(ecology, "geom")
    val urbanRDD = Adapter.toSpatialRdd(urban, "geom")
    slopeRDD.analyze()
    slopeRDD.spatialPartitioning(GridType.KDBTREE, 20)
    cultivatedRDD.spatialPartitioning(slopeRDD.getPartitioner) //根据已有的分区进行空间分区，无需analyze
    ecologyRDD.spatialPartitioning(slopeRDD.getPartitioner)
    urbanRDD.spatialPartitioning(slopeRDD.getPartitioner)
    cultivatedRDD.buildIndex(IndexType.QUADTREE, true) //耕地建索引
    slopeRDD.buildIndex(IndexType.QUADTREE, true)
    val inters1 = new SpatialRDD[Geometry]()
    //    inters1.setRawSpatialRDD(JoinQuery.SpatialJoinQueryFlat(cultivatedRDD, ecologyRDD, true, SpatialPredicate.INTERSECTS)
    //      .map { pair =>
    //        pair._1.intersection(pair._2)
    //      }.filter(g => g.isValid))
    //疑问：JoinQuery.SpatialJoinQueryFlat得到的结果是否保留空间分区？
    val erase1 = new SpatialRDD[Geometry]()
    val pairRDD1 = JoinQuery.SpatialJoinQuery(ecologyRDD, cultivatedRDD, true, SpatialPredicate.INTERSECTS)
    val pairRDD1part1 = pairRDD1.map { pair =>
      val (inputGeom, eraseGeoms) = (pair._1, pair._2)
      val mergedEraseGeom = eraseGeoms.foldLeft(eraseGeoms.get(0)) { (current, geom) =>
        current.union(geom)
      }
      (inputGeom.intersection(mergedEraseGeom), inputGeom.difference(mergedEraseGeom))
    }.filter(g => g._1.isValid && g._2.isValid)
    inters1.setRawSpatialRDD(pairRDD1part1.map(_._1).cache())
    val pairRDD1part2: JavaRDD[Geometry] = cultivatedRDD.rawSpatialRDD.subtract(pairRDD1.map(i=>i._1).toJavaRDD()) //相离的要素
    erase1.setRawSpatialRDD(pairRDD1part1.map(_._2).union(pairRDD1part2))
    erase1.spatialPartitioning(slopeRDD.getPartitioner)
    erase1.buildIndex(IndexType.QUADTREE, true)
    val inters2 = new SpatialRDD[Geometry]()
    //    inters2.setRawSpatialRDD(JoinQuery.SpatialJoinQueryFlat(erase1, urbanRDD, true, SpatialPredicate.INTERSECTS)
    //      .map { pair =>
    //        pair._1.intersection(pair._2)
    //      }.filter(g => g.isValid))
    val erase2 = new SpatialRDD[Geometry]()
    val pairRDD2 = JoinQuery.SpatialJoinQuery(urbanRDD, erase1, true, SpatialPredicate.INTERSECTS)
    val pairRDD2part1 = pairRDD2.map { pair =>
      val (inputGeom, eraseGeoms) = (pair._1, pair._2)
      val mergedEraseGeom = eraseGeoms.foldLeft(eraseGeoms.get(0)) { (current, geom) =>
        current.union(geom)
      }
      (inputGeom.intersection(mergedEraseGeom), inputGeom.difference(mergedEraseGeom))
    }.filter(g => g._1.isValid && g._2.isValid)
    inters2.setRawSpatialRDD(pairRDD2part1.map(_._1).cache())
    val pairRDD2part2: JavaRDD[Geometry] = erase1.rawSpatialRDD.subtract(pairRDD2.map(i=>i._1).toJavaRDD()) //相离的要素
    erase2.setRawSpatialRDD(pairRDD2part1.map(_._2).union(pairRDD2part2))
    erase2.spatialPartitioning(slopeRDD.getPartitioner)
    erase2.buildIndex(IndexType.QUADTREE, true)
    val inters3 = new SpatialRDD[Geometry]()
    //    inters3.setRawSpatialRDD(JoinQuery.SpatialJoinQueryFlat(erase2, slopeRDD, true, SpatialPredicate.INTERSECTS)
    //      .map { pair =>
    //        pair._1.intersection(pair._2)
    //      }.filter(g => g.isValid))
    val erase3 = new SpatialRDD[Geometry]()
    val pairRDD3 = JoinQuery.SpatialJoinQuery(slopeRDD, erase2, true, SpatialPredicate.INTERSECTS)
    val pairRDD3part1 = pairRDD3.map { pair =>
      val (inputGeom, eraseGeoms) = (pair._1, pair._2)
      val mergedEraseGeom = eraseGeoms.foldLeft(eraseGeoms.get(0)) { (current, geom) =>
        current.union(geom)
      }
      (inputGeom.intersection(mergedEraseGeom), inputGeom.difference(mergedEraseGeom))
    }.filter(g => g._1.isValid && g._2.isValid)
    inters3.setRawSpatialRDD(pairRDD3part1.map(_._1).cache())
    //inters3可以用flatMap，省下一次SpatialJoinQueryFlat
    val pairRDD3part2: JavaRDD[Geometry] = erase2.rawSpatialRDD.subtract(pairRDD3.map(i=>i._1).toJavaRDD()) //相离的要素
    erase3.setRawSpatialRDD(pairRDD3part1.map(_._2).union(pairRDD3part2).cache())
    // 考虑在关键地方加一些缓存
    val bufferRawSpatialRDD = erase3.rawSpatialRDD.filter(g => g.getArea < 3333.3333).map(g => g.buffer(10))
    val buffer = new SpatialRDD[Geometry]()
    buffer.setRawSpatialRDD(bufferRawSpatialRDD)
    buffer.analyze()
    buffer.spatialPartitioning(GridType.KDBTREE, 20) //可以考虑变化一下试试效果
    erase3.spatialPartitioning(buffer.getPartitioner)
    //可以考虑索引也改成KDBTREE
    buffer.buildIndex(IndexType.QUADTREE, true)
    erase3.buildIndex(IndexType.QUADTREE, true)
    val fragment = JoinQuery.SpatialJoinQuery(erase3, buffer, true, SpatialPredicate.INTERSECTS)
      .map{ pair =>
        val (buffer, others) = (pair._1, pair._2)
        if (others.size == 1) (others.get(0), 0.0)
        else{
          val geom: Geometry = others.find(geom => geom.buffer(10).equals(buffer)).get
          val sumArea = others.aggregate(0.0)((area: Double, geom: Geometry) => (area + geom.getArea),
            (area1: Double, area2: Double) => area1 + area2)
          val periArea = sumArea - geom.getArea
          (geom, periArea)
        }
      }.filter(_._2 < 6666.6667).map(tuple => tuple._1)
    val fragmentRDD = new SpatialRDD[Geometry]()
    fragmentRDD.setRawSpatialRDD(fragment)

    Adapter.toDf(inters1, sedona).withColumn("reason", lit("ecology"))
      .unionByName(Adapter.toDf(inters2, sedona).withColumn("reason", lit("urban")))
      .unionByName(Adapter.toDf(inters3, sedona).withColumn("reason", lit("slope")))
      .unionByName(Adapter.toDf(fragmentRDD, sedona).withColumn("reason", lit("fragment")))
      .withColumnRenamed("geometry", "geom")
      .withColumn("area", expr("ST_Area(geom)")).filter(col("area") > 200)
      .withColumn("geom", expr("ST_Transform(geom, 'epsg:4527', 'epsg:4490')"))

  }

  def main(args: Array[String]): Unit = {
    case class Student(classId: Int, name: String, gender: String, age: Int)
    val spark = SparkSession.builder().master("local[*]").appName("testAgg").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
//    load(sc, )
//    val stuDF: DataFrame = Seq(
//      Student(1001, "zhangsan", "F", 20),
//      Student(1002, "lisi", "M", 16),
//      Student(1003, "wangwu", "M", 21),
//      Student(1004, "zhaoliu", "F", 21),
//      Student(1004, "zhouqi", "M", 22),
//      Student(1001, "qianba", "M", 19),
//      Student(1003, "liuliu", "F", 23)).toDF
    print("hello")
//    print(aggregate_min(stuDF, "age"))

  }
}
