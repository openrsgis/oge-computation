package whu.edu.cn.plugin.api

import org.apache.spark.SparkContext
import whu.edu.cn.entity.AllowedData

import scala.collection.mutable

/**
 * 算子统一执行接口
 */
trait MethodExecutor {

  /**
   * 返回算子名称
   *
   * @return 算子名称
   */
  def name(): String

  /**
   * 返回算子版本
   *
   * @return 算子版本
   */
  def version(): String

  /**
   * 执行算子<br>
   * 该函数表示一个算子的整个执行逻辑，从输入、处理执行到输出
   * 执行计算任务时，每一个DAG节点就对应一个execute方法被调用
   *
   * @param context Spark上下文对象，隐式自动传递
   * @param args    参数列表，其中key是参数名称，值是对应参数具体值，需进行向下转型
   * @return 算子执行结果
   */
  def execute(implicit context: SparkContext, args: mutable.Map[String, AllowedData]): AllowedData

}