package whu.edu.cn.util

import org.apache.commons.lang3.StringUtils

/**
 * 命名使用工具类
 */
object NameUtils {

  /**
   * 将驼峰命名转换为全大写蛇形命名，例如：
   * <ul>
   * <li>user -> USER</li>
   * <li>userService -> USER_SERVICE</li>
   * <li>granuleJSON -> GRANULE_JSON</li>
   * <li>parseXMLData -> PARSE_XML_DATA</li>
   * <ul>
   *
   * @param input 输入的驼峰命名法名称
   * @return
   */
  def camelToScreamingSnakeCase(input: String): String = {
    if (StringUtils.isEmpty(input)) {
      return ""
    }
    // 在小写字母和大写字母之间插入下划线
    val step1 = input.replaceAll("([a-z])([A-Z])", "$1_$2")
    // 在连续大写字母后接小写字母的地方插入下划线
    val step2 = step1.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
    // 转换为大写
    step2.toUpperCase
  }

}