package whu.edu.cn.plugin

import com.typesafe.scalalogging.StrictLogging
import whu.edu.cn.plugin.api.MethodExecutor

import java.util.ServiceLoader
import scala.collection.mutable

/**
 * 基于SPI机制的插件加载器
 */
object PluginLoader extends StrictLogging {

  /**
   * 存放所有已注册算子列表<br>
   * <ul>
   * <li>键：注册的算子名称</li>
   * <li>值：已注册的算子实例，即MethodExecutor的实现类</li>
   * </ul>
   */
  val executors = mutable.Map.empty[String, MethodExecutor]

  /**
   * 基于SPI机制，扫描并初始化全部算子，即实现了MethodExecutor的类并实例化
   * 需要在静态块中调用，作为初始化方法
   */
  private def load(): Unit = {
    // 使用SPI机制加载全部实现类
    val loadedExecutors = ServiceLoader.load(classOf[MethodExecutor])
    // 注册到算子注册列表
    executors.clear()
    loadedExecutors.forEach(item => executors.put(item.name(), item))
    logger.info(s"插件加载器已扫描并注册${executors.size}个算子！")
  }

  {
    load()
  }

}