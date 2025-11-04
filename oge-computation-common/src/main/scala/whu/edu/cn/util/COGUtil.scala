package whu.edu.cn.util

import com.baidubce.services.bos.BosClient
import com.baidubce.services.bos.model.GetObjectRequest
import com.typesafe.scalalogging.LazyLogging
import geotrellis.layer.SpatialKey
import geotrellis.vector.Extent
import geotrellis.vector.reproject.Reproject
import io.minio.{GetObjectArgs, MinioClient}
import org.apache.commons.io.IOUtils
import org.locationtech.jts.geom.{Envelope, Geometry}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.BosConf.BOS_BUCKET_NAME
import whu.edu.cn.config.GlobalConfig.MinioConf.{MINIO_BUCKET_NAME, MINIO_HEAD_SIZE}
import whu.edu.cn.entity.{CoverageMetadata, RawTile}

import java.io.{ByteArrayOutputStream, InputStream}
import scala.collection.mutable


// noinspection AccessorLikeMethodIsUnit
// TODO lrx: 这个类后面要改成Scala版本的
// TODO lrx: 这里要确保每个瓦片的赋予的范围是原始坐标系下的，比如32649必须是米，包括分辨率也必须是原始坐标系下的，以米为单位的数值 =>已经确保，分别在t.setResolution和t.setExtent处
// TODO lrx: 这里要检查Extent和Resolution的单位和值对不对

abstract class COGUtil extends Serializable with LazyLogging {

  var tileDifference = 0
  var tmsLevel = 0 // Scaling levels of the front-end TMS
  var extent: Extent = _
  final val TypeArray: Array[Int] = Array( //"???",
    0, //
    1, // byte //8-bit unsigned integer
    1, // ascii//8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero)
    2, // short",2),//16-bit (2-byte) unsigned integer.
    4, // long",4),//32-bit (4-byte) unsigned integer.
    8, // rational",8),//Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
    1, // sbyte",1),//An 8-bit signed (twos-complement) integer
    1, // undefined",1),//An 8-bit byte that may contain anything, depending on the definition of the field
    2, // sshort",1),//A 16-bit (2-byte) signed (twos-complement) integer.
    4, // slong",1),// A 32-bit (4-byte) signed (twos-complement) integer.
    8, // srational",1),//Two SLONG’s: the first represents the numerator of a fraction, the second the denominator.
    4, // float",4),//Single precision (4-byte) IEEE format
    8 // double",8)//Double precision (8-byte) IEEE format
  )

  def tileQuery(client: Any, level: Int, coverageMetadata: CoverageMetadata, windowsExtent: Extent, queryGeometry: Geometry, bandCounts: Int*): mutable.ArrayBuffer[RawTile]

  def getTileBuf(client: Any, tile: RawTile): RawTile

  def getTileBufArray(client: Any, tile: RawTile): Array[Byte]

  /**
   * 获取 Tile 相关的一些数据，不包含tile影像本体
   *
   * @param level      json里的 level 字段，表征前端 Zoom
   * @param queryGeometry
   * @param crs
   * @param path
   * @param time
   * @param measurement
   * @param dType
   * @param resolution 数据库中的原始影像分辨率
   * @param productName
   * @return
   */
  def getTiles(level: Int, coverageMetadata: CoverageMetadata,
               tileOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]],
               cell: mutable.ArrayBuffer[Double], geoTrans: mutable.ArrayBuffer[Double],
               tileByteCounts: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]],
               bandCount: Int,
               windowsExtent: Extent,
               queryGeometry: Geometry): mutable.ArrayBuffer[RawTile] = {
    var tileLevel: Int = 0
    var resolutionTMS: Double = .0
    // 地图 zoom 为0时的分辨率，以下按zoom递增
    val resolutionTMSArray: Array[Double] = Array(156543.033928, 78271.516964, 39135.758482, 19567.879241, 9783.939621, 4891.969810, 2445.984905, 1222.992453, 611.496226, 305.748113, 152.874057, 76.437028, 38.218514, 19.109257, 9.554629, 4.777314, 2.388657, 1.194329, 0.597164, 0.298582, 0.149291)
    val resolutionOrigin: Double = coverageMetadata.getResolution
    if (level == -1) {
      tileLevel = 0
    } else {
      // 计算TMS分辨率
      resolutionTMS = resolutionTMSArray(level)
      // 利用分辨率 计算tileLevel ,设置本次的 level范围
      tileLevel = Math.floor(Math.log(resolutionTMS / resolutionOrigin) / Math.log(2)).toInt + 1
      if (tileLevel > tileOffsets.length - 1) {
        tileDifference = tileOffsets.length - 1 - tileLevel
        tileLevel = tileOffsets.length - 1
      }
      else if (tileLevel < 0) {
        tileDifference = -tileLevel
        tileLevel = 0
      }
    }
    logger.info(s"level = $level resolutionOrigin= $resolutionOrigin ,tileLevel $tileLevel,tileOffsets.length =${tileOffsets.length}")
    // 使用窗口范围的全局变量

    val visualExtent = if (windowsExtent != null) Reproject(windowsExtent, CRSUtils.getCrsByName("EPSG:4326"), coverageMetadata.getCrs) else null
    val queryEnv: Envelope = queryGeometry.getEnvelopeInternal
    val queryMbr: Extent = Reproject(queryEnv, CRSUtils.getCrsByName("EPSG:4326"), coverageMetadata.getCrs)

    // 将传入的范围改为数据所在坐标系下，方便两个范围进行相交
    // 传入的范围始终是 4326 坐标系下的
    val queryMbrReproj: Extent = Extent(queryMbr.xmin, queryMbr.ymin, queryMbr.xmax, queryMbr.ymax)

    extent = queryMbrReproj
    // 图像范围
    // 东西方向空间分辨率  --->像素宽度
    val wReso: Double = cell(0)
    // 南北方向空间分辨率 ---> 像素高度
    val hReso: Double = cell(1)
    println("wReso = " + wReso)
    println("hReso = " + hReso)

    // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
    val xMin: Double = geoTrans(3)
    val yMax: Double = geoTrans(4)
    val tiles: mutable.ArrayBuffer[RawTile] = mutable.ArrayBuffer.empty[RawTile]

    //计算目标影像的左上和右下图上坐标
    val pLeft: Int = ((queryMbrReproj.xmin - xMin) / (256 * wReso * Math.pow(2, tileLevel).toInt)).toInt
    val pRight: Int = ((queryMbrReproj.xmax - xMin) / (256 * wReso * Math.pow(2, tileLevel).toInt)).toInt
    val pLower: Int = ((yMax - queryMbrReproj.ymax) / (256 * hReso * Math.pow(2, tileLevel).toInt)).toInt
    val pUpper: Int = ((yMax - queryMbrReproj.ymin) / (256 * hReso * Math.pow(2, tileLevel).toInt)).toInt
    val srcSize: Array[Double] = Array[Double](wReso, hReso)
    val pRange: Array[Double] = Array[Double](xMin, yMax)


    for (i <- Math.max(pLower, 0) to (
      if (pUpper >= tileOffsets(tileLevel).length / bandCount) tileOffsets(tileLevel).length / bandCount - 1
      else pUpper
      )) {
      for (j <- Math.max(pLeft, 0) to (
        if (pRight >= tileOffsets(tileLevel)(i).length) tileOffsets(tileLevel)(i).length - 1
        else pRight)
           ) {
        for (k <- 0 until bandCount) {
          val extent = new Extent(
            j * (256 * srcSize(0) * Math.pow(2, tileLevel)) + pRange(0),
            (i + 1) * (256 * -srcSize(1) * Math.pow(2, tileLevel)) + pRange(1),
            (j + 1) * (256 * srcSize(0) * Math.pow(2, tileLevel)) + pRange(0),
            i * (256 * -srcSize(1) * Math.pow(2, tileLevel)) + pRange(1))
          if (visualExtent == null || (visualExtent != null && extent.intersects(visualExtent))) {
            val t = new RawTile
            val kPlus: Int = tileOffsets(tileLevel).length / bandCount
            t.setOffset(tileOffsets(tileLevel)(i + k * kPlus)(j))
            t.setByteCount(tileByteCounts(tileLevel)(i)(j))
            t.setExtent(extent)
            t.setRotation(geoTrans(5))
            // 前端的分辨率只是为了查询TMS层级的
            t.setResolutionCol(wReso * Math.pow(2, tileLevel))
            t.setResolutionRow(hReso * Math.pow(2, tileLevel))
            t.setSpatialKey(new SpatialKey(j - pLeft, i - pLower))
            // 使用 coverageMetadata 填充参数
            t.setCoverageId(coverageMetadata.getCoverageID)
            t.setPath(coverageMetadata.getPath)
            t.setTime(coverageMetadata.getTime)
            t.setCrs(coverageMetadata.getCrs)
            t.setDataType(coverageMetadata.getDataType)
            t.setProduct(coverageMetadata.getProduct)
            t.setMeasurement(coverageMetadata.getMeasurement)
            if (bandCount == 1) {
              t.setMeasurementRank(coverageMetadata.getMeasurementRank)
            } else {
              t.setMeasurementRank(coverageMetadata.getMeasurementRank + bandCount)
            }
            tiles.append(t)
          }
        }
      }
    }
    logger.info(s" measurement: ${coverageMetadata.measurement} .tile size :${tiles.size}, tileLevel: $tileLevel")
    tiles
  }

  /**
   * 解析参数，并修改一些数据
   *
   * @param header
   * @param tileOffsets
   * @param cell
   * @param geoTrans
   * @param tileByteCounts
   * @param imageSize 图像尺寸
   */
  def parse(header: Array[Byte], tileOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]], cell: mutable.ArrayBuffer[Double], geoTrans: mutable.ArrayBuffer[Double], tileByteCounts: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]], imageSize: mutable.ArrayBuffer[Int], bandCount: Int): Unit = {
    // DecodeIFH
    var pIFD: Int = getIntII(header, 4, 4)
    // DecodeIFD
    while (pIFD != 0) {
      val DECount: Int = getIntII(header, pIFD, 2)
      pIFD += 2
      for (_ <- 0 until DECount) {
        // DecodeDE
        val TagIndex: Int = getIntII(header, pIFD, 2)
        val TypeIndex: Int = getIntII(header, pIFD + 2, 2)
        val Count: Int = getIntII(header, pIFD + 4, 4)
        // 先找到数据的位置
        var pData: Int = pIFD + 8
        val totalSize: Int = TypeArray(TypeIndex) * Count
        if (totalSize > 4) {
          pData = getIntII(header, pData, 4)
        }
        // 再根据Tag把值读出并存起来，GetDEValue
        getDEValue(TagIndex, TypeIndex, Count, pData, header, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount)
        // 之前的
        pIFD += 12
      }
      pIFD = getIntII(header, pIFD, 4)
    }
  }

  def getDEValue(tagIndex: Int, typeIndex: Int, count: Int, pData: Int, header: Array[Byte], tileOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]], cell: mutable.ArrayBuffer[Double], geoTrans: mutable.ArrayBuffer[Double], tileByteCounts: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]], imageSize: mutable.ArrayBuffer[Int], bandCount: Int): Unit = {
    val typeSize: Int = TypeArray(typeIndex)
    tagIndex match {
      case 256 => //ImageWidth
        imageSize(1) = getIntII(header, pData, typeSize)
      case 257 => //ImageLength
        imageSize(0) = getIntII(header, pData, typeSize)
      case 258 =>
        val BitPerSample: Int = getIntII(header, pData, typeSize)
      case 286 => //XPosition
        val xPosition: Int = getIntII(header, pData, typeSize)
      case 287 => //YPosition
        val yPosition: Int = getIntII(header, pData, typeSize)
      case 324 => //tileOffsets
        getOffsetArray(pData, typeSize, header, tileOffsets, imageSize, bandCount)
      case 325 => //tileByteCounts
        getTileBytesArray(pData, typeSize, header, tileByteCounts, imageSize, bandCount)
      case 33550 => //  cellWidth
        getDoubleCell(pData, typeSize, count, header, cell)
      case 33922 => //geoTransform
        getDoubleTrans(pData, typeSize, count, header, geoTrans)
      case 34737 => //Spatial reference
        val crs: String = getString(header, pData, typeSize * count)
      case _ =>
    }
  }

  def getIntII(pd: Array[Byte], startPos: Int, Length: Int): Int = {
    var value: Int = 0
    for (i <- 0 until Length) {
      value |= pd(startPos + i) << i * 8
      if (value < 0) {
        value += 256 << i * 8
      }
    }
    value
  }

  def getLong(pd: Array[Byte], startPos: Int, Length: Int): Long = {
    var value: Long = 0
    for (i <- 0 until Length) {
      value |= (pd(startPos + i) & 0xff).toLong << (8 * i)
      if (value < 0) value += 256.toLong << i * 8
    }
    value
  }

  def getDouble(pd: Array[Byte], startPos: Int, Length: Int): Double = {
    var value: Long = 0
    for (i <- 0 until Length) {
      value |= (pd(startPos + i) & 0xff).toLong << (8 * i)
      if (value < 0) value += 256.toLong << i * 8
    }
    java.lang.Double.longBitsToDouble(value)
  }

  def getDoubleTrans(startPos: Int, typeSize: Int, count: Int, header: Array[Byte], geoTrans: mutable.ArrayBuffer[Double]): Unit = {
    for (i <- 0 until count) {
      val v: Double = getDouble(header, startPos + i * typeSize, typeSize)
      geoTrans.append(v)
    }
  }

  def getDoubleCell(startPos: Int, typeSize: Int, count: Int, header: Array[Byte], cell: mutable.ArrayBuffer[Double]): Unit = {
    for (i <- 0 until count) {
      val v: Double = getDouble(header, startPos + i * typeSize, typeSize)
      cell.append(v)
    }
  }

  def getString(pd: Array[Byte], startPos: Int, Length: Int): String = {
    val dd = new Array[Byte](Length)
    System.arraycopy(pd, startPos, dd, 0, Length)
    new String(dd)
  }

  def getOffsetArray(startPos: Int, typeSize: Int, header: Array[Byte], tileOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]], imageSize: mutable.ArrayBuffer[Int], bandCount: Int): Unit = {
    val StripOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[Long]] = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[Long]]
    for (_ <- 0 until bandCount) {
      for (i <- 0 until (imageSize(0) / 256) + 1) {
        val Offsets: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty[Long]
        for (j <- 0 until (imageSize(1) / 256) + 1) {
          val v: Long = getLong(header, startPos + (i * ((imageSize(1) / 256) + 1) + j) * typeSize, typeSize)
          Offsets.append(v)
        }
        StripOffsets.append(Offsets)
      }
    }
    tileOffsets.append(StripOffsets)
  }

  def getTileBytesArray(startPos: Int, typeSize: Int, header: Array[Byte], tileByteCounts: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]], imageSize: mutable.ArrayBuffer[Int], bandCount: Int): Unit = {
    val stripBytes: mutable.ArrayBuffer[mutable.ArrayBuffer[Long]] = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[Long]]
    for (_ <- 0 until bandCount) {
      for (i <- 0 until (imageSize(0) / 256) + 1) {
        val tileBytes: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty[Long]
        for (j <- 0 until (imageSize(1) / 256) + 1) {
          val v: Long = getLong(header, startPos + (i * ((imageSize(1) / 256) + 1) + j) * typeSize, typeSize)
          tileBytes.append(v)
        }
        stripBytes.append(tileBytes)
      }
    }
    tileByteCounts.append(stripBytes)
  }

}

class COGUtil_Bos extends COGUtil {
  /**
   * 根据元数据查询 tile
   *
   * @param level         JSON中的level字段，前端层级
   * @param path          获取tile的路径
   * @param time
   * @param crs
   * @param measurement   影像的测量方式
   * @param dType
   * @param resolution
   * @param productName
   * @param queryGeometry 查询瓦片的矩形范围
   * @param bandCounts    多波段
   * @return 后端瓦片
   */
  override def tileQuery(client: Any, level: Int, coverageMetadata: CoverageMetadata, windowsExtent: Extent, queryGeometry: Geometry, bandCounts: Int*): mutable.ArrayBuffer[RawTile] = {
    tmsLevel = level
    var bandCount = 1
    if (bandCounts.length > 1) throw new RuntimeException("bandCount 参数最多传一个")
    if (bandCounts.length == 1) bandCount = bandCounts(0)

    // 一些头文件变量
    val imageSize: mutable.ArrayBuffer[Int] = new mutable.ArrayBuffer[Int](2) // imageLength & imageWidth
    imageSize.append(0)
    imageSize.append(0)
    val tileByteCounts: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]] = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]]
    val geoTrans: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.empty[Double]
    val cell: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.empty[Double]
    val tileOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]] = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]]

    val getObjectRequest: GetObjectRequest = new GetObjectRequest(BOS_BUCKET_NAME, coverageMetadata.getPath)
    getObjectRequest.setRange(0, MINIO_HEAD_SIZE)
    try {
      val headerByte: Array[Byte] = client.asInstanceOf[BosClient].getObjectContent(getObjectRequest)
      parse(headerByte, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount)

      val tiles = getTiles(level, coverageMetadata, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, windowsExtent, queryGeometry)
      COGUtil.setParams(tileDifference, tmsLevel, extent)
      tiles

    } catch {
      case e: Exception =>
        throw new Exception("所请求的数据在Bos中不存在！", e)
    }
  }

  /**
   * 获取 tile 影像本体
   *
   * @param tile tile相关数据
   * @return
   */
  override def getTileBuf(client: Any, tile: RawTile): RawTile = {
    val getObjectRequest: GetObjectRequest = new GetObjectRequest(GlobalConfig.Others.bucketName, tile.getPath)
    getObjectRequest.setRange(tile.getOffset, tile.getOffset + tile.getByteCount)
    try {
      val byteArray: Array[Byte] = client.asInstanceOf[BosClient].getObjectContent(getObjectRequest)
      tile.setTileBuf(byteArray)
      tile
    } catch {
      case e: Exception =>
        logger.info(
          s"请求 BOS失败. " +
            s"Path: ${tile.getPath}, " +
            s"Range: [${tile.getOffset}, ${tile.getOffset + tile.getByteCount}] " +
            s",请求的 url ${client.asInstanceOf[BosClient].getEndpoint}", e
        )
        val byteArray: Array[Byte] = Array.emptyByteArray
        tile.setTileBuf(byteArray)
        tile
    }
  }

  override def getTileBufArray(client: Any, tile: RawTile): Array[Byte] = {
    val getObjectRequest: GetObjectRequest = new GetObjectRequest(GlobalConfig.Others.bucketName, tile.getPath)
    getObjectRequest.setRange(tile.getOffset, tile.getOffset + tile.getByteCount)
    try {
      client.asInstanceOf[BosClient].getObjectContent(getObjectRequest)
    } catch {
      case e: Exception =>
        logger.info(
          s"请求 BOS失败. " +
            s"Path: ${tile.getPath}, " +
            s"Range: [${tile.getOffset}, ${tile.getOffset + tile.getByteCount}] " +
            s",请求的 url ${client.asInstanceOf[BosClient].getEndpoint}", e
        )
        Array.emptyByteArray
    }
  }

}

class COGUtil_Minio extends COGUtil {
  /**
   * 根据元数据查询 tile
   *
   * @param level         JSON中的level字段，前端层级
   * @param path          获取tile的路径
   * @param time
   * @param crs
   * @param measurement   影像的测量方式
   * @param dType
   * @param resolution
   * @param productName
   * @param queryGeometry 查询瓦片的矩形范围
   * @param bandCounts    多波段
   * @return 后端瓦片
   */
  override def tileQuery(client: Any, level: Int, coverageMetadata: CoverageMetadata, windowsExtent: Extent, queryGeometry: Geometry, bandCounts: Int*): mutable.ArrayBuffer[RawTile] = {
    tmsLevel = level
    var bandCount = 1
    if (bandCounts.length > 1) throw new RuntimeException("bandCount 参数最多传一个")
    if (bandCounts.length == 1) bandCount = bandCounts(0)

    // 一些头文件变量
    val imageSize: mutable.ArrayBuffer[Int] = new mutable.ArrayBuffer[Int](2) // imageLength & imageWidth
    imageSize.append(0)
    imageSize.append(0)
    val tileByteCounts: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]] = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]]
    val geoTrans: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.empty[Double]
    val cell: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.empty[Double]
    val tileOffsets: mutable.ArrayBuffer[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]] = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[mutable.ArrayBuffer[Long]]]
    //    try{
    val inputStream: InputStream = client.asInstanceOf[MinioClient].getObject(GetObjectArgs.builder.bucket(MINIO_BUCKET_NAME).`object`(coverageMetadata.getPath).offset(0L).length(MINIO_HEAD_SIZE).build)
    // Read data from stream
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](MINIO_HEAD_SIZE)
    var len: Int = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) {
      outStream.write(buffer, 0, len)
    }
    val headerByte: Array[Byte] = outStream.toByteArray
    outStream.close()
    inputStream.close()

    parse(headerByte, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount)
    val tiles = getTiles(level, coverageMetadata, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, windowsExtent, queryGeometry)
    COGUtil.setParams(tileDifference, tmsLevel, extent)
    tiles

  }

  /**
   * 获取 tile 影像本体
   *
   * @param tile tile相关数据
   * @return
   */
  override def getTileBuf(client: Any, tile: RawTile): RawTile = {
    var inputStream: InputStream = null
    try {
      val minioClient = client.asInstanceOf[MinioClient]
      val args = GetObjectArgs.builder()
        .bucket(MINIO_BUCKET_NAME)
        .`object`(tile.getPath)
        .offset(tile.getOffset)
        .length(tile.getByteCount)
        .build()

      // 使用try-with-resources风格自动关闭流
      inputStream = minioClient.getObject(args)
      val buffer = new Array[Byte](tile.getByteCount.toInt)
      IOUtils.readFully(inputStream, buffer)
      tile.setTileBuf(buffer)
    } catch {
      case exception: Exception
      => logger.error(
        s"请求 Minio失败. " +
          s"Path: ${tile.getPath}, " +
          s"Range: [${tile.getOffset}, ${tile.getOffset + tile.getByteCount}]", exception
      )
    } finally {
      if (inputStream != null) try {
        inputStream.close()
      } catch {
        case closingEx: Exception =>
          logger.warn("关闭输入流时发生异常", closingEx)
      }
    }
    tile
  }

  override def getTileBufArray(client: Any, tile: RawTile): Array[Byte] = {
    var inputStream: InputStream = null
    try {
      val minioClient = client.asInstanceOf[MinioClient]
      val args = GetObjectArgs.builder()
        .bucket(MINIO_BUCKET_NAME)
        .`object`(tile.getPath)
        .offset(tile.getOffset)
        .length(tile.getByteCount)
        .build()

      // 使用try-with-resources风格自动关闭流
      inputStream = minioClient.getObject(args)
      val buffer = new Array[Byte](tile.getByteCount.toInt)
      IOUtils.readFully(inputStream, buffer)
      return buffer
    } catch {
      case exception: Exception
      => logger.error(
        s"请求 Minio失败. " +
          s"Path: ${tile.getPath}, " +
          s"Range: [${tile.getOffset}, ${tile.getOffset + tile.getByteCount}]", exception
      )
    } finally {
      if (inputStream != null) try {
        inputStream.close()
      } catch {
        case closingEx: Exception =>
          logger.warn("关闭输入流时发生异常", closingEx)
      }
    }
    Array.emptyByteArray
  }
}

// 创建工厂对象
object COGUtil {
  var tileDifference = 0
  var tmsLevel = 0 // Scaling levels of the front-end TMS
  var extent: Extent = _

  def createCOGUtil(serviceType: String): COGUtil = {
    serviceType.toLowerCase match {
      case "minio" => new COGUtil_Minio()
      case "bos" => new COGUtil_Bos()
      case _ => throw new IllegalArgumentException("Invalid service type")
    }
  }

  def setParams(tileDiff: Int, tmsLvl: Int, ext: Extent): Unit = {
    tileDifference = tileDiff
    tmsLevel = tmsLvl
    extent = ext
  }
}
