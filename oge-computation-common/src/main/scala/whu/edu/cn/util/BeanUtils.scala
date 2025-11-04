package whu.edu.cn.util

import org.apache.commons.lang3.StringUtils

object BeanUtils {

  /**
   * 判断值是否为空
   *
   * @param value 传入值
   * @return 是否为空，数值类型-1视为空
   */
  private def isEmpty(value: Any): Boolean = {
    value match {
      case _: Int | _: Double | _: Float | _: Long | _: Short | _: Byte => value == -1
      case _: String => value == null || value == ""
      case _ => value == null
    }
  }

  /**
   * 判断是否是简单值类型
   *
   * @param value 传入值
   * @return 是否是简单值类型（数字、布尔或者字符串）
   */
  private def isValueType(value: Any): Boolean = {
    value match {
      case _: Int | _: Double | _: Float | _: Long | _: Short | _: Byte | _: Char | _: Boolean | _: String => true
      case _ => false
    }
  }

  /**
   * 融合配置对象，如果：
   * <ul>
   * <li>newObject某字段值与origin相等，或者newObject某字段值为空，则跳过</li>
   * <li>newObject某字段值与origin不相同，则覆盖到origin对应字段上面</li>
   * </ul>
   *
   * @param origin    原始配置对象
   * @param newObject 新配置对象
   */
  def mergeConfig[T <: AnyRef](origin: T, newObject: T): Unit = {
    // 遍历字段
    val fields = origin.getClass.getDeclaredFields
    fields.filter(field => {
      // 过滤掉新值为空的字段
      field.setAccessible(true)
      !isEmpty(field.get(newObject))
    }).foreach(field => {
      field.setAccessible(true)
      // 获取两个对象字段值
      val originValue = field.get(origin)
      val newValue = field.get(newObject)
      // 判断类型，若为值类型，且值不同则进行替换
      if (isValueType(newValue)) {
        if (originValue != newValue) {
          field.set(origin, newValue)
        }
      } else {
        // 否则说明是引用类型对象，递归对比
        mergeConfig(originValue, newValue)
      }
    })
  }

  /**
   * 反射输出对象所有字段极其值
   *
   * @param obj         一个对象
   * @param prefix      输出前缀，用于递归层级缩进
   * @param ignoreEmpty 忽略空值
   */
  def printFields(obj: AnyRef, prefix: String = "", ignoreEmpty: Boolean = true): Unit = {
    if (obj == null) {
      return
    }
    // 遍历字段
    val fields = obj.getClass.getDeclaredFields
    fields.foreach(field => {
      field.setAccessible(true)
      val value = field.get(obj)
      // 判断空
      if (isEmpty(value)) {
        // 直接输出
        if (!ignoreEmpty) {
          println(s"$prefix${field.getName}: $value")
        }
      } else {
        // 不为空则根据类型进行处理
        // 基本类型则直接输出
        if (isValueType(value)) {
          println(s"$prefix${field.getName}: $value")
        } else {
          // 引用类型则递归输出
          println(s"$prefix${field.getName}:")
          printFields(value, s"$prefix  ")
        }
      }
    })
  }

  /**
   * 将字符串类型转换成其它基本数据类型
   *
   * @param value      输入字符串
   * @param targetType 目标转换类型
   */
  def convertString[T](value: String, targetType: Class[T]): T = {
    val result: Any = targetType match {
      case c if c == classOf[String] => value
      case c if c == classOf[Int] => value.toInt
      case c if c == classOf[Long] => value.toLong
      case c if c == classOf[Short] => value.toShort
      case c if c == classOf[Byte] => value.toByte
      case c if c == classOf[Float] => value.toFloat
      case c if c == classOf[Double] => value.toDouble
      case c if c == classOf[Boolean] => value.toBoolean
      case _ => throw new IllegalArgumentException("Unsupported type")
    }
    result.asInstanceOf[T]
  }

  /**
   * 根据对象属性名称，从环境变量读取值并赋值给对应属性
   * 对于传入对象obj，传入前缀prefix，连接符conjunction为-，那么：
   * <ul>
   * <li>obj的name属性，会读取环境变量PREFIX-NAME的值并赋值</li>
   * <li>obj的userId属性，会读取环境变量PREFIX-USER_ID的值并赋值</li>
   * <li>obj嵌套一个对象inner，对于inner的name属性，会读取环境变量PREFIX-INNER-NAME的值并赋值</li>
   * <ul>
   * 若对应属性无对应名称的环境变量，则跳过该字段
   *
   * @param obj         值注入对象
   * @param prefix      前缀，无前缀可传入空字符串
   * @param conjunction 连接符，默认为_
   */
  def environmentVariableInject(obj: AnyRef, prefix: String = "", conjunction: String = "_"): Unit = {
    // 反射读取属性
    if (obj == null) {
      return
    }
    val fields = obj.getClass.getDeclaredFields
    fields.foreach(field => {
      field.setAccessible(true)
      var envName = field.getName
      if (!StringUtils.isEmpty(prefix)) {
        envName = s"$prefix$conjunction$envName"
      }
      // 值类型直接获取对应值
      val fieldValue = field.get(obj)
      if (isValueType(fieldValue)) {
        val envValue = System.getenv(NameUtils.camelToScreamingSnakeCase(envName))
        if (!StringUtils.isEmpty(envValue)) {
          field.set(obj, convertString(envValue, field.getType))
        }
      } else {
        // 引用类型则递归赋值
        environmentVariableInject(fieldValue, envName)
      }
    })
  }

}