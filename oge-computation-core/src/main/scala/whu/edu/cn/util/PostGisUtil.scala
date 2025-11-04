package whu.edu.cn.util

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.oge.OGEGeometry

import java.sql.Statement

object PostGisUtil {
//  val statement = getStatement()

  def getStatement(): Statement = {
    val statement = PostgresqlUtilDev.getConnection.createStatement()
    // 启用PostGIS扩展
    statement.execute("CREATE EXTENSION IF NOT EXISTS plpgsql")
    statement.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    //  statement.execute("CREATE EXTENSION IF NOT EXISTS postgis_raster") // OPTIONAL
    statement.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology") // OPTIONAL
    statement
  }
  def enableSpatialIndex(statement: Statement,  tableName: String, spatialIndexType: String = "GIST", geometryField: String = "geom"): Unit = {
    // 启用PostGIS扩展
    statement.execute("CREATE EXTENSION IF NOT EXISTS plpgsql")
    statement.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    statement.execute("CREATE EXTENSION IF NOT EXISTS postgis_raster") // OPTIONAL
    statement.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology") // OPTIONAL
    statement.execute(s"alter table ${tableName} alter column ${geometryField} type geometry")
    //在geometryField列上构建GIST空间索引
    statement.execute(s"CREATE INDEX IF NOT EXISTS geom_idx ON ${tableName} USING ${spatialIndexType} ( ${geometryField} ); ")
    statement.close()
  }



  //查询整表数据
  def loadFromPostGIS(sedona: SparkSession, tableName: String, geometryField: String = "geom"): DataFrame = {
    SedonaSQLRegistrator.registerAll(sedona)
    val jdbcOpts: Map[String, String] = Map(
      "url" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_URL,
      "user" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_USER,
      "password" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_PWD,
      "dbtable" -> tableName
    )
    val df = sedona.read
      .format("jdbc")
      .options(jdbcOpts)
      .load()
      .withColumn("geom", expr(s"ST_GeomFromWKB($geometryField)"))
    df
  }
  def loadFromPostGISWithoutGeometry(sedona: SparkSession, tableName: String): DataFrame = {
    SedonaSQLRegistrator.registerAll(sedona)
    val jdbcOpts: Map[String, String] = Map(
      "url" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_URL,
      "user" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_USER,
      "password" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_PWD,
      "dbtable" -> tableName
    )
    val df = sedona.read.format("jdbc")
      .options(jdbcOpts)
      .load()
    df
  }
  //自定义SQL语句查询
  def selectUsingSQL(sedona: SparkSession, query: String, geometryField: String = "geom"): DataFrame = {
    SedonaSQLRegistrator.registerAll(sedona)
    val jdbcOpts: Map[String, String] = Map(
      "url" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_URL,
      "user" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_USER,
      "password" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_PWD,
      "query" -> query
    )
    val df = sedona.read.format("jdbc")
      .options(jdbcOpts)
      .load()
      .withColumn("geom", expr(s"ST_GeomFromWKB(${geometryField})"))
    df
  }
  def selectUsingSQLWithoutGeometry(sedona: SparkSession, query: String): DataFrame = {
    SedonaSQLRegistrator.registerAll(sedona)
    val jdbcOpts: Map[String, String] = Map(
      "url" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_URL,
      "user" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_USER,
      "password" -> GlobalConfig.PostgreSqlConf.POSTGRESQL_PWD,
      "query" -> query
    )
    val df = sedona.read.format("jdbc")
      .options(jdbcOpts)
      .load()
    df
  }
  //查询落在OGEGeometry对象范围内的数据
  def selectByGeometry(sedona: SparkSession, tableName: String, geometry: OGEGeometry): DataFrame = {
    //使用索引感知函数：ST_Contains、ST_ContainsProperly、ST_CoveredBy、ST_Covers、ST_Crosses、ST_Intersects、ST_Overlaps、
    // ST_Touches、ST_Within、ST_Within 和 ST_3DIntersects，ST_DWithin、ST_DFullyWithin、ST_3DDFullyWithin 和 ST_3DDWithin
    val geometryText = geometry.geometryToWKT()
    val query: String = s"SELECT * FROM ${tableName} WHERE ST_Contains(ST_GeomFromText(‘${geometryText}’), ${tableName}.geom)"
    selectUsingSQL(sedona, query)
  }
  def selectByTime(sedona: SparkSession, tableName: String, startTime: String, endTime: String): DataFrame = {
    //使用索引感知函数：ST_Contains、ST_ContainsProperly、ST_CoveredBy、ST_Covers、ST_Crosses、ST_Intersects、ST_Overlaps、
    // ST_Touches、ST_Within、ST_Within 和 ST_3DIntersects，ST_DWithin、ST_DFullyWithin、ST_3DDFullyWithin 和 ST_3DDWithin
    val query: String = s"SELECT * FROM ${tableName} WHERE time >= ${startTime} AND time <= ${endTime}"
    selectUsingSQL(sedona, query)
  }
  def selectByGeometryAndTime(sedona: SparkSession, tableName: String, geometry: OGEGeometry, startTime: String, endTime: String): DataFrame = {
    //使用索引感知函数：ST_Contains、ST_ContainsProperly、ST_CoveredBy、ST_Covers、ST_Crosses、ST_Intersects、ST_Overlaps、
    // ST_Touches、ST_Within、ST_Within 和 ST_3DIntersects，ST_DWithin、ST_DFullyWithin、ST_3DDFullyWithin 和 ST_3DDWithin
    val geometryText = geometry.geometryToWKT()
    val query: String = s"SELECT * FROM ${tableName} WHERE ST_Contains(ST_GeomFromText(‘${geometryText}’), ${tableName}.geom) AND time >= ${startTime} AND time <= ${endTime}"
    selectUsingSQL(sedona, query)
  }





  //这个函数有问题
//  def insertIntoTable(tableName: String, insertValues: Map[String, Any]) = {
//    //插入
//    val keys = insertValues.keys.mkString(",")
//    val values =insertValues.values.mkString(",")
//    val insertQuery: String = s"INSERT INTO ${tableName} (${keys}) VALUES (?, ?, ?)"
//    val connection = PostgresqlUtilDev.getConnection
//    val preparedStatement: PreparedStatement = connection.prepareStatement(insertQuery)
//    // 设置参数
//    preparedStatement.setString(1, "value1")
//    preparedStatement.setInt(2, 123)
//    preparedStatement.setString(3, "value3")
//    // 执行插入操作
//    val rowsAffected: Int = preparedStatement.executeUpdate
//    // 关闭连接
//    preparedStatement.close()
//    connection.close()
//  }

  def saveToPostGIS(sedona: SparkSession, df: DataFrame, tableName: String, spatialIndex: Boolean = true, geometryField: String = "geom") = {
    val statement: Statement = getStatement()
    val tableExists = checkTableExists(statement, tableName) // 检查表是否存在
    if (!tableExists) createTable(statement, tableName, geometryField) //创建表

    df.withColumn(geometryField, expr(s"ST_AsWKB(${geometryField})"))
      .write.format("jdbc").mode("overwrite")
      .option("url", GlobalConfig.PostgreSqlConf.POSTGRESQL_URL)
      .option("user", GlobalConfig.PostgreSqlConf.POSTGRESQL_USER)
      .option("password", GlobalConfig.PostgreSqlConf.POSTGRESQL_PWD)
      .option("dbtable", tableName)
      .option("driver", "org.postgresql.Driver") // 添加 JDBC 驱动
      //      .option("truncate","true") // 表示在写入数据之前清空现有表的内容，而不是重新创建表。这可以保留表结构和索引
      .save()
    statement.execute(s"ANALYZE ${tableName};") //要求 PostgreSQL 遍历该表并更新其用于查询计划估计的内部统计信息
    statement.close()
  }

  private def checkTableExists(statement: Statement, tableName: String): Boolean = {
    val resultSet = statement.executeQuery(s"SELECT to_regclass('${tableName}')")
    resultSet.next() && resultSet.getString(1) != null
  }

  private def createTable(statement: Statement, tableName: String, geometryField: String): Unit = {
    // 假设 DataFrame 中的其他列是 (id INT, name TEXT, geom GEOMETRY)
    // 你需要根据实际的 DataFrame 列定义来调整 SQL 语句
    val createTableSQL = s"""
      CREATE TABLE ${tableName} (
        id SERIAL PRIMARY KEY,
        name TEXT,
        ${geometryField} GEOMETRY
      )
    """
    statement.execute(createTableSQL)
  }


}
