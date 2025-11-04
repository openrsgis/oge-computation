package whu.edu.cn.util

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils

import java.io.InputStream
import java.nio.charset.StandardCharsets


/**
 * 读取jar包内文件（classpath路径下）的实用类
 */
object ClasspathUtils extends StrictLogging {

  /**
   * 读取为InputStream
   *
   * @param classpath jar包内路径，/表示根路径
   * @param clazz     参照类
   * @return 文件输入流
   */
  def readAsStream[T](classpath: String, clazz: Class[T]): InputStream = {
    clazz.getResourceAsStream(classpath)
  }

  /**
   * 读取为二进制字节数组
   *
   * @param classpath jar包内路径，/表示根路径
   * @param clazz     参照类
   * @return 文件输入流
   */
  def readAsBytes[T](classpath: String, clazz: Class[T]): Array[Byte] = {
    val stream = readAsStream(classpath, clazz)
    try {
      IOUtils.toByteArray(stream)
    } catch {
      case e: Exception =>
        logger.error(s"读取文件classpath:${classpath}错误！class=${clazz.getName}", e)
        throw e
    } finally {
      stream.close()
    }
  }

  /**
   * 读取为字符串
   *
   * @param classpath jar包内路径，/表示根路径
   * @param clazz     参照类
   * @return 文件输入流
   */
  def readAsString[T](classpath: String, clazz: Class[T]): String = {
    new String(readAsBytes(classpath, clazz), StandardCharsets.UTF_8)
  }

}