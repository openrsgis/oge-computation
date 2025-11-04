package whu.edu.cn.util

import com.typesafe.scalalogging.StrictLogging
import geotrellis.proj4.CRS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.CoverageMetadata
import whu.edu.cn.oge.{Feature, OGEGeometry}

import java.sql.{Connection, ResultSet, Statement}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex

object PostgresqlServiceUtil extends StrictLogging {
  // 定义SQL转义函数
  def escapeSql(value: String): String = {
    if (value == null || value.isEmpty) {
      return value
    }

    // 1. 解码 \\uXXXX 格式的Unicode
    // Regex to find patterns like \\u5c71
    val unicodePattern: Regex = """\\u([0-9a-fA-F]{4})""".r

    val decodedValue = unicodePattern.replaceAllIn(value, m => {
      // 将匹配到的16进制码转为字符
      val hexCode = m.group(1)
      Integer.parseInt(hexCode, 16).toChar.toString
    })

    // 2. 转义SQL中的单引号
    decodedValue.replace("'", "''")
  }

  def queryCoverageCollection(productName: String,
                              sensorName: String = null,
                              measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String],
                              startTime: String = null,
                              endTime: String = null,
                              extent: Geometry = null,
                              crs: CRS = null,
                              cloudCoverMin: Float = 0,
                              cloudCoverMax: Float = 100,
                              bands: ArrayBuffer[String] = ArrayBuffer.empty[String],
                              coverageIDList: ArrayBuffer[String] = ArrayBuffer.empty[String]
                             ): ListBuffer[CoverageMetadata] = {
    val metaData = new ListBuffer[CoverageMetadata]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = new StringBuilder(
          """
            |SELECT
            |  oge_image.product_id,
            |  oge_image.image_identification,
            |  oge_image.crs,
            |  oge_image.path,
            |  ST_AsText (oge_image.geom ) AS geom,
            |  oge_image.phenomenon_time,
            |  oge_data_product.name,
            |  oge_data_product.description,
            |  oge_image_product.dtype,
            |  oge_image_product_band.band_num,
            |  oge_image_product_band.band_rank,
            |  oge_image_product_band.band_train,
            |  oge_image_product_band.resolution as resolution_m
            |FROM
            |  oge_image
            |  JOIN oge_data_product ON oge_image.product_id = oge_data_product.id
            |  JOIN oge_image_product ON oge_image.product_id = oge_image_product.product_id
            |  JOIN oge_image_product_band ON oge_image.product_id =oge_image_product_band.product_id
            |--   这里原始sql中select和where中未用到oge_measurement
            |--   JOIN oge_measurement ON oge_product_measurement.measurement_key = oge_measurement.measurement_key
            |""".stripMargin
        )

        // 收集WHERE条件
        val conditions = mutable.ListBuffer[String]()



        // 添加必填条件 - 处理逗号分隔的产品名称
        if (productName != null && productName.nonEmpty) {
          val productNames = productName.split(",").map(_.trim).filter(_.nonEmpty)
          if (productNames.length == 1) {
            // 单个产品名称使用等号
            conditions += s"name = '${escapeSql(productNames.head)}'"
          } else if (productNames.length > 1) {
            // 多个产品名称使用IN语句
            val escapedNames = productNames.map(name => s"'${escapeSql(name)}'").mkString(", ")
            conditions += s"name IN ($escapedNames)"
          }
        } else {
          logger.warn("productName is null")
          return metaData
        }
        conditions += "oge_image_product_band.band_train != 0"

        if (sensorName != null && sensorName.nonEmpty) {
          conditions += s"sensor_name = '${escapeSql(sensorName)}'"
        }

        if (measurementName.nonEmpty) {
          val measurements = measurementName.map(m => s"'${escapeSql(m)}'").mkString(", ")
          conditions += s"band_num IN ($measurements)"
        }

        if (startTime != null) {
          conditions += s"phenomenon_time >= '${escapeSql(startTime)}'"
        }

        if (endTime != null) {
          conditions += s"'${escapeSql(endTime)}' >= phenomenon_time"
        }

        if (crs != null) {
          conditions += s"crs = '${escapeSql(crs.toString)}'"
        }

        if (cloudCoverMin != null && cloudCoverMax != null) {
          // 假设cover_cloud是数值类型，去掉引号
          conditions += s"cover_cloud >= $cloudCoverMin"
          conditions += s"cover_cloud <= $cloudCoverMax"
        }

        if (extent != null) {
          val wkt = geotrellis.vector.io.wkt.WKT.write(extent)
          conditions += s"ST_Intersects(geom, 'SRID=4326;$wkt')"
        }
        if (bands != null && bands.nonEmpty) {
          val escapedBands = bands.map(b => s"'${escapeSql(b)}'").mkString(", ")
          conditions += s"band_num in ($escapedBands)"
        }
        // 添加coverageIDList条件
        if (coverageIDList != null && coverageIDList.nonEmpty) {
          val escapedCoverage = coverageIDList.filter(t => t.nonEmpty).map(b => s"'${escapeSql(b)}'").mkString(", ")
          if (escapedCoverage.nonEmpty) {
            conditions += s"image_identification in ($escapedCoverage)"
          }
        }

        // 拼接WHERE子句
        if (conditions.nonEmpty) {
          sql ++= "WHERE " + conditions.mkString(" AND ")
        }
        val extentResults: ResultSet = statement.executeQuery(sql.toString())
        println(sql.toString())
        // 排除BQA波段
        while (extentResults.next()) {
          val coverageMetadata = new CoverageMetadata
          coverageMetadata.setCoverageID(extentResults.getString("image_identification"))
          coverageMetadata.setMeasurement(extentResults.getString("band_num"))
          coverageMetadata.setMeasurementRank(extentResults.getInt("band_rank"))
          coverageMetadata.setPath(extentResults.getString("path") + "/" + coverageMetadata.getCoverageID + "_" + coverageMetadata.getMeasurement + ".tif")
          coverageMetadata.setTime(extentResults.getString("phenomenon_time"))
          coverageMetadata.setCrs(CRSUtils.getCrsByName(extentResults.getString("crs")))
          coverageMetadata.setGeom(extentResults.getString("geom"))
          coverageMetadata.setDataType(extentResults.getString("dtype"))
          coverageMetadata.setProduct(extentResults.getString("name"))
          coverageMetadata.setProductDescription(extentResults.getString("description"))
          coverageMetadata.setResolution(extentResults.getDouble("resolution_m"))
          metaData.append(coverageMetadata)

        }
      }
      finally {
        conn.close()
      }
    } else throw new RuntimeException("connection failed")
    if (metaData.isEmpty) {
      throw new RuntimeException("There is no data in the query range.")
    }
    metaData
  }

  def queryCoverage(coverageId: String, productKey: String): ListBuffer[CoverageMetadata] = {
    val metaData = new ListBuffer[CoverageMetadata]
    val productKey2 = escapeSql(productKey)
    // 查询数据并处理
    PostgresqlUtilDev.simpleSelect(
      resultNames = Array(
        "oge_image.product_id", "oge_image.image_identification",
        "oge_image.crs", "oge_image.path", "st_astext(oge_image.geom)"
      ),
      tableName = "oge_image",
      rangeLimit = Array(
        ("image_identification", "=", coverageId),
        ("name", "=", productKey2)
      ),
      aliases = Array(
        "geom",
        "oge_image.phenomenon_time",
        "oge_data_product.name",
        "oge_image_product.dtype",
        "oge_image_product_band.band_num",
        "oge_image_product_band.band_rank",
        "oge_image_product_band.band_train",
        "oge_image_product_band.resolution as resolution_m "
      ),
      jointLimit = Array(
        ("oge_data_product",
          " oge_data_product.id=oge_image.product_id "),
        ("oge_image_product_band",
          "oge_image_product_band.product_id = oge_image.product_id "),
        ("oge_image_product",
          " oge_image_product.product_id=oge_image.product_id ")
      ),
      func = extentResults => {
        // 排除BQA波段
        while (extentResults.next()) {
          //去掉这个字段
//          if (extentResults.getInt("band_train") != 0) {
            val coverageMetadata = new CoverageMetadata
            coverageMetadata.setCoverageID(extentResults.getString("image_identification"))
            coverageMetadata.setMeasurement(extentResults.getString("band_num"))
            coverageMetadata.setMeasurementRank(extentResults.getInt("band_rank"))
            coverageMetadata.setPath(extentResults.getString("path") + "/" + coverageMetadata.getCoverageID + "_" + coverageMetadata.getMeasurement + ".tif")
            coverageMetadata.setGeom(extentResults.getString("geom"))
            coverageMetadata.setTime(extentResults.getString("phenomenon_time"))
            coverageMetadata.setCrs(CRSUtils.getCrsByName(extentResults.getString("crs")))
            coverageMetadata.setDataType(extentResults.getString("dtype"))
            coverageMetadata.setResolution(extentResults.getDouble("resolution_m"))
            metaData.append(coverageMetadata)
            logger.info("请求文件路径:" + coverageMetadata.getPath)

        }
      }
    )
    metaData
  }

  //查询单条数据，不利用Sedona的SQL语句
  def queryFeature(featureId: String, tableName: String): Feature = {
    val selectSQL =
      s"""
         |SELECT *,
         |       ST_GeometryType(geom) AS type,
         |       ST_SRID(geom) AS srid
         |FROM $tableName
         |WHERE id = '$featureId';
     """.stripMargin
    val statement: Statement = PostGisUtil.getStatement()
    val resultSet: ResultSet = statement.executeQuery(selectSQL)
    var feature: Feature = new Feature()
    while (resultSet.next()) {
      val id = resultSet.getString("id")
      val geometry = new OGEGeometry(resultSet.getObject("geom").asInstanceOf[Geometry])
      val map = mutable.Map.empty[String, Any]
      val columnCount = resultSet.getMetaData.getColumnCount
      for (i <- 1 to columnCount) {
        val label = resultSet.getMetaData.getColumnLabel(i)
        if (!Set("id", "geom", "type", "srid").contains(label)) {
          map += (label -> resultSet.getObject(i))
        }
      }
      feature = new Feature(geometry, map, id)
    }
    resultSet.close()
    statement.close()
    feature
  }


  def queryFeatureCollection(spark: SparkSession, tableName: String, geometry: OGEGeometry, startTime: String, endTime: String): DataFrame = {
    if (geometry == null && startTime == null && endTime == null) {
      PostGisUtil.loadFromPostGIS(spark, tableName)
    }
    else {
      var query: String =
        if (geometry == null && startTime == null && endTime == null) s"SELECT * FROM ${tableName}"
        else s"SELECT * FROM ${tableName} WHERE "
      if (geometry != null) {
        val geometryWKT = geometry.geometryToWKT()
        query += s"ST_Contains(ST_GeomFromText('${geometryWKT}', ${geometry.geometry.getSRID}), ${tableName}.geom)"
      }
      if (geometry != null && startTime != null) query += s" AND time >= '${startTime}'"
      else if (geometry == null && startTime != null) query += s"time >= '${startTime}'"
      if ((geometry != null || startTime != null) && startTime != null) query += s" AND time <= '${endTime}'"
      else if ((geometry == null || startTime == null) && startTime != null) query += s"time <= '${endTime}'"
      PostGisUtil.enableSpatialIndex(PostGisUtil.getStatement(), tableName)
      PostGisUtil.selectUsingSQL(spark, query)
    }
  }
}
