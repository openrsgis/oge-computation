package whu.edu.cn.util

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.geocube.util.HbaseUtil.{getVectorCell, getVectorMeta}
import whu.edu.cn.oge.{Feature, OGEGeometry}

import scala.collection.mutable.Map

object HbaseServiceUtil {
  def getFeatureWithRowkey(hbaseTableName: String, rowKey: String): Feature = {
    val t1 = System.currentTimeMillis()
    val meta = getVectorMeta(hbaseTableName, rowKey, "vectorData", "metaData")
    val cell = getVectorCell(hbaseTableName, rowKey, "vectorData", "geom")
    //    println(meta)
    //    println(cell)
    val t2 = System.currentTimeMillis()
    println("根据rowkey取数据时间：" + (t2 - t1) / 1000)
    val jsonObject = JSON.parseObject(meta)
    val properties = jsonObject.getJSONArray("properties").getJSONObject(0)
    val propertiesOut = Map.empty[String, Any]
    val sIterator = properties.keySet.iterator
    while (sIterator.hasNext()) {
      val key = sIterator.next();
      val value = properties.getString(key);
      propertiesOut += (key -> value)
    }
    val reader = new WKTReader()
    val geometry: Geometry = reader.read(cell)
    new Feature(new OGEGeometry(geometry), propertiesOut)
  }
  def getFeatureCollection(spark: SparkSession, hbaseTableName: String, productKey: String, geometry: OGEGeometry, startTime: String, endTime: String): DataFrame = {
    // 定义 HBase 列映射关系
//    import spark.implicits.to
    val hbase_column_mapping = "id STRING :key, " +
      "geom STRING vectorData:geom, " +
      "metaData STRING vectorData:metaData, "
    // 使用 Spark 读取 HBase 表中的数据，根据列映射关系转换为 DataFrame
//    new HBaseContext(spark.sparkContext, HbaseUtil.configuration)
    val hbaseDF = spark.read
      .format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping", hbase_column_mapping)
//      .option("hbase.spark.pushdown.columnfilter", false) // 禁用列过滤下推优化
      .option("hbase.table", hbaseTableName)
      .load()
    hbaseDF
  }

  def saveFeatureCollection(spark: SparkSession, hbaseTableName: String, productKey: String): Unit = {
  }

}
