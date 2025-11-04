package whu.edu.cn.util

import org.geotools.data.{DefaultTransaction, FeatureReader, FeatureWriter, Transaction}
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.feature.FeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.{HashMap => JHashMap}

object ShpConverter {

  /**
   * 将 GeoJSON 文件转换为 Shapefile.
   *
   * @param geojsonFile 输入的 GeoJSON 文件
   * @param shpFile     输出的 Shapefile 文件
   */
  def convert(geojsonFile: File, shpFile: File): Unit = {
    // --- 步骤 1: 读取 GeoJSON 数据源 ---
    var shpStore: ShapefileDataStore = null
    var featureWriter: FeatureWriter[SimpleFeatureType, SimpleFeature] = null

    try {
      println(s"开始读取 GeoJSON 文件: ${geojsonFile.getAbsolutePath}")
      val fjson = new FeatureJSON()
      val features = fjson.readFeatureCollection(geojsonFile).asInstanceOf[FeatureCollection[SimpleFeatureType, SimpleFeature]]


      val factory = new ShapefileDataStoreFactory()
      val params = new JHashMap[String, java.io.Serializable]()
      params.put("url", shpFile.toURI.toURL)
      params.put("create spatial index", java.lang.Boolean.TRUE)

      shpStore = factory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
      // 设置 Shapefile 的字符编码，避免中文乱码
      shpStore.setCharset(StandardCharsets.UTF_8)

      val sourceSchema = features.getSchema

      val shpSchemaBuilder = new SimpleFeatureTypeBuilder()
      shpSchemaBuilder.setName(sourceSchema.getName)

      try {
        val crs = sourceSchema.getCoordinateReferenceSystem
        if (crs != null) {
          shpSchemaBuilder.setCRS(crs)
        } else {
          shpSchemaBuilder.setCRS(CRS.decode("EPSG:4326"))
        }
      } catch {
        case e: Exception =>
          println(s"警告: 设置坐标系时出错: ${e.getMessage}")
        // 继续处理，即使坐标系设置失败
      }

      shpSchemaBuilder.add("the_geom", sourceSchema.getDescriptor("geometry").getType.getBinding)

      sourceSchema.getAttributeDescriptors.forEach { descriptor =>
        var attrName = descriptor.getLocalName
        if (!"geometry".equals(attrName)) {
          if (attrName.length > 10) {
            println(s"警告: 字段名 '${attrName}' 过长，将被截断为前10个字符。")
            attrName = attrName.substring(0, 10)
          }
          shpSchemaBuilder.add(attrName, descriptor.getType.getBinding)
        }
      }
      val shpSchema = shpSchemaBuilder.buildFeatureType()

      shpStore.createSchema(shpSchema)

      featureWriter = shpStore.getFeatureWriter(shpStore.getTypeNames()(0), Transaction.AUTO_COMMIT)

      val interator = features.features()

      println("开始写入特征数据...")
      var count = 0
      while (interator.hasNext) {
        val sourceFeature = interator.next()

        val feature = featureWriter.next()

        sourceFeature.getProperties.forEach { prop =>
          val propName = prop.getName.getLocalPart
          var shpAttrName = propName
          if (shpAttrName.length > 10) {
            shpAttrName = shpAttrName.substring(0, 10)
          }
          if ("geometry".equals(propName)) {
            shpAttrName = "the_geom"
          }
          // 确保字段存在于目标schema中
          if (shpSchema.getDescriptor(shpAttrName) != null) {
            feature.setAttribute(shpAttrName, sourceFeature.getAttribute(propName))
          }
        }

        // 写入新特征
        featureWriter.write()
        count += 1
      }

      println(s"写入完成！总共写入 ${count} 个特征。")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // --- 步骤 7: 关闭所有资源，非常重要！ ---
      println("正在关闭资源...")
      if (featureWriter != null) featureWriter.close()
      if (shpStore != null) shpStore.dispose()
    }
  }

  def main(args: Array[String]): Unit = {
    // --- 使用示例 ---
    // 假设您的项目根目录下有 "data" 文件夹
    val geojsonPath = "D:\\02-data\\testwh\\0.geojson"
    val shpPath = "D:\\02-data\\testwh\\3.shp"

    // 定义输出的Shapefile路径
    val f1 = new File(geojsonPath)
    val f2 = new File(shpPath)

    // 执行转换
    convert(f1, f2)
  }
}
