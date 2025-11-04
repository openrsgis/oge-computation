package whu.edu.cn.config

import org.scalatest.funsuite.AnyFunSuite
import whu.edu.cn.config.GlobalConfig.{appConfig, loadConfig}
import whu.edu.cn.util.BeanUtils

class ConfigTests extends AnyFunSuite {

  test("测试加载配置") {
    loadConfig()
    BeanUtils.printFields(appConfig)
  }

}