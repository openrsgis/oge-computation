package whu.edu.cn.jsonparser

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.entity.Node
import whu.edu.cn.jsonparser.JsonToArg.BackAndOn
import whu.edu.cn.util.HttpRequestUtil
import whu.edu.cn.util.{ClasspathUtils, HttpRequestUtil}

import java.io.File
import scala.collection.mutable
import scala.util.control.Breaks
import scala.collection.JavaConverters._

// TODO lrx: 解析的时候加上数据类型？

/**
 * 解析DAG JSON参数的工具类型
 */
object JsonToArg extends StrictLogging {

  /**
   * 算子JSON Schema文件位置，用于本地调试，读取时：
   * 1.首先从文件系统读取
   * 2.文件系统不存在，则从classpath的跟路径/读取
   */
  var jsonAlgorithms: String = ""

  /**
   * 存放每个操作的DAG节点的映射表
   */
  var dagMap: mutable.Map[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]] = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]]


  /**
   * 根据算子名称获取算子配置
   *
   * @param functionName 算子名称
   * @return 算子配置的JSON对象
   */
  def getAlgorithmJson(functionName: String): JSONObject = {
    // 兼容本地local模式调试
    if (jsonAlgorithms.nonEmpty) {
      var content = ""
      // 先从配置的文件系统路径读取
      val jsonAlgorithmFile = new File(jsonAlgorithms)
      if (jsonAlgorithmFile.exists()) {
        content = FileUtils.readFileToString(jsonAlgorithmFile)
      } else {
        // 否则，从classpath的跟路径下读取
        logger.warn(s"Algorithm JSON Schema File：$jsonAlgorithms is not exists in local filesystem, trying read from classpath...")
        content = ClasspathUtils.readAsString(s"/$jsonAlgorithms", this.getClass)
      }
      val json = JSON.parseObject(content)
      return json.getJSONObject(functionName)
    }
    // 集群计算环境
    // 若列表已包含对应函数操作则返回

    // 请求服务获取算子配置信息
    val url = s"${GlobalConfig.ServiceConf.BASE_URL}model-resource/getModelParamJson?functionName=${functionName}"
    val response = HttpRequestUtil.sendGet(url)
    println(s"请求url $url, 返回结果 $response")
    val responseJson = JSON.parseObject(response)
    if (!responseJson.getString("code").equals("20000")) {
      val message = s"Web API request failed! Url: $url, response: $responseJson"
      logger.error(message)
      throw new RuntimeException(message)
    }
    // 解析算子配置
    val configObject = try {
      JSON.parseObject(responseJson.getString("data"))
    } catch {
      case e: Exception =>
        val message = s"Failed to parse JSON content, algorithmFile: $jsonAlgorithms, functionName: $functionName, responseJSON: $responseJson"
        logger.error(message)
        throw new RuntimeException(message, e)
    }
    configObject
  }

  /**
   * 获取一个函数参数数量
   *
   * @param functionName 函数名
   * @return 对应函数名的参数数量
   */
  def numberOfArgs(functionName: String): Int = {
    getAlgorithmJson(functionName).getJSONArray("args").size
  }

  /**
   * 根据下标获取一个函数的参数名称
   *
   * @param functionName 函数名
   * @param index        参数下标
   * @return 对应下标的对应函数参数名
   */
  def getArgNameByIndex(functionName: String, index: Int): String = {
    getAlgorithmJson(functionName).getJSONArray("args").getJSONObject(index).getString("name")
  }

  /**
   * 获取JSON对象中的值引用
   *
   * @param valueReference 属性名
   * @param jsonObject     一个JSON对象
   * @return JSON对象中引用对象
   */
  def getValueReference(valueReference: String, jsonObject: JSONObject): JSONObject = {
    jsonObject.getJSONObject(valueReference)
  }

  //原先BackAndOn不支持复杂嵌套，例如双重arrayValue，对其进行修改
  /**
    * 根据算子名称获取算子配置
    *
    * @param node 当前处于哪个Node节点之下
    * @param input 当前待解析的JSONObject
    * @param jsonObject 原始的完整的JSONObject
    * @return
    */
  def parseJsonObject(node: Node, input: JSONObject, jsonObject: JSONObject, paraI: Int, j: Int = 0): JSONObject = {
    if (input.containsKey("valueReference")) {
      val value: JSONObject = getValueReference(input.getString("valueReference"), jsonObject)
      return parseJsonObject(node, value, jsonObject, paraI, j)
    }
    if (input.containsKey("functionInvocationValue")) {
      if (input.getJSONObject("functionInvocationValue") != null) {
        val nodeChildren: Node = new Node
        nodeChildren.setFunctionName(input.getJSONObject("functionInvocationValue").getString("functionName"))
        nodeChildren.setArguments(input.getJSONObject("functionInvocationValue").getJSONObject("arguments"))
        nodeChildren.setParent(node)
        nodeChildren.setDepth(node.getDepth + 1)
        nodeChildren.setWidth(paraI+1)
        if (j>0) nodeChildren.setUUID(node.getUUID + paraI.toString + (j-1).toString)
        else nodeChildren.setUUID(node.getUUID + paraI.toString)
        node.addChildren(nodeChildren)
        BackAndOn(nodeChildren, jsonObject)
        input.remove("functionInvocationValue")
        input.put("constantValue", nodeChildren.getUUID)
      }
    }
    if (input.containsKey("arrayValue")) {
      val nodeArray: JSONArray = input.getJSONObject("arrayValue").getJSONArray("values")
      for (i <- (0 until nodeArray.size).reverse) {
        val key: JSONObject = nodeArray.getJSONObject(i)
        nodeArray.remove(i)
        nodeArray.add(i, parseJsonObject(node, key, jsonObject, paraI, i+1))
      }
    }
    if (input.containsKey("dictionaryValue")) {
      val nodeDic: JSONObject = input.getJSONObject("dictionaryValue").getJSONObject("values")
      var i = 0
      for (s <- nodeDic.keySet().asScala.toList) {
        val key: JSONObject = nodeDic.getJSONObject(s)
        nodeDic.remove(s)
        nodeDic.put(s, parseJsonObject(node, key, jsonObject, paraI, i+1))
        i += 1
      }
    }
    input
  }

  def BackAndOn(node: Node, jsonObject: JSONObject): Node = {
    for (i <- 0 until numberOfArgs(node.getFunctionName)) {
      val keys: JSONObject = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i))
      if (keys != null) {
        if (keys.containsKey("functionDefinitionValue")) { //keys是第i个参数（JSONObject）
          val body: String = keys.getJSONObject("functionDefinitionValue").getString("body")
          trans(jsonObject, body)
        }
        if (keys.containsKey("functionInvocationValue")) {
          val nodeChildren: Node = new Node
          nodeChildren.setFunctionName(keys.getJSONObject("functionInvocationValue").getString("functionName"))
          nodeChildren.setArguments(keys.getJSONObject("functionInvocationValue").getJSONObject("arguments"))
          nodeChildren.setParent(node)
          nodeChildren.setDepth(node.getDepth + 1)
          nodeChildren.setWidth(i + 1)
          nodeChildren.setUUID(node.getUUID + i.toString)
          node.addChildren(nodeChildren)
          BackAndOn(nodeChildren, jsonObject)
        }
        if (keys.containsKey("valueReference")) {
          val value: JSONObject = getValueReference(keys.getString("valueReference"), jsonObject)
          val parsedValue = parseJsonObject(node, value, jsonObject, i) //第i个参数
          //valueReference的对象内仅可能有一个键值对
          val keyOfParsedValue = parsedValue.keySet().asScala.toList.head
          node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).remove("valueReference")
          node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).put(keyOfParsedValue, parsedValue.get(keyOfParsedValue))
        }
        if (keys.containsKey("arrayValue")) {
          val nodeArray: JSONArray = keys.getJSONObject("arrayValue").getJSONArray("values")
          for (j <- (0 until nodeArray.size).reverse) {
            val key: JSONObject = nodeArray.getJSONObject(j)
            nodeArray.remove(j)
            nodeArray.add(j, parseJsonObject(node, key, jsonObject, i, j+1))
          }
        }
        if (keys.containsKey("dictionaryValue")) {
          val nodeDic: JSONObject = keys.getJSONObject("dictionaryValue").getJSONObject("values")
          var j = 0
          for (s <- nodeDic.keySet().asScala.toList) {
            val key: JSONObject = nodeDic.getJSONObject(s)
            nodeDic.remove(s)
            nodeDic.put(s, parseJsonObject(node, key, jsonObject, i, j+1))
            j += 1
          }
        }
      }
    }
    node
  }

  def DFS(nodeList: List[Node], arg: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): List[Node] = {
    val node: Iterator[Node] = nodeList.iterator
    while (node.hasNext) {
      val nodeNow: Node = node.next
      if (nodeNow.getChildren != null) {
        DFS(nodeNow.getChildren, arg)
      }
      writeAsTuple(nodeNow, arg)
    }
    nodeList
  }

  def parseValuetoString(input: JSONObject): String = {
    if (input.containsKey("arrayValue")) {
      val getArray: JSONArray = input.getJSONObject("arrayValue").getJSONArray("values")
      var st: String = "["
      for (i <- 0 until getArray.size) {
        val get: String = parseValuetoString(getArray.getJSONObject(i))
        st = st + get + ", "
      }
      if (getArray.size != 0) st = st.dropRight(2)
      st = st + "]"
      st
    }
    else if (input.containsKey("dictionaryValue")) {
      val getDic: JSONObject = input.getJSONObject("dictionaryValue").getJSONObject("values")
      var st: String = "{"
      for (s <- getDic.keySet().asScala.toList) {
        val get: AnyRef = parseValuetoString(getDic.getJSONObject(s))
        st = st + s + ":" + get + ", "
      }
      if (getDic.size()!=0) st = st.dropRight(2)
      st = st + "}"
      st
    }
    else if (input.containsKey("constantValue")) {
      input.getString("constantValue")
      val get: AnyRef = input.get("constantValue")
      val getString: String = input.getString("constantValue")
      if (get.isInstanceOf[JSONArray]) {
        getString.replace("\"", "")
      }
      else {
        getString
      }
    }
    else if (input.containsKey("argumentReference")) {
      input.getString("argumentReference")
      val get: AnyRef = input.get("argumentReference")
      val getString: String = input.getString("argumentReference")
      if (get.isInstanceOf[JSONArray]) {
        getString.replace("\"", "")
      }
      else {
        getString
      }
    }
    else if (input.containsKey("functionDefinitionValue")) {
      input.getJSONObject("functionDefinitionValue").getString("body")
    }
    else ""
  }

  def writeAsTuple(node: Node, arg: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): Unit = {
    val UUID: String = node.getUUID
    val name: String = node.getFunctionName
    val map = mutable.Map.empty[String, String] //(参数名, UUID)
    arg.append(Tuple3(UUID, name, map)) //例如：(0,FeatureCollection.export,Map(featureCollection -> 00))
    val num: Int = numberOfArgs(node.getFunctionName) //当前函数在注册文件中的参数个数
    val size: Int = node.getArguments.size //当前函数实际参数个数
    var sizeCount = 1
    val loop = new Breaks
    loop.breakable {
      for (i <- 0 until num) {
        if (node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)) != null) {
          val keys: JSONObject = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)) //第i个参数对应的JSONObject
          if (keys.containsKey("functionInvocationValue") || keys.containsKey("valueReference")) {
            if (sizeCount <= size) {
              map += (getArgNameByIndex(node.getFunctionName, i) -> (node.getUUID + i.toString))
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
          else if (keys.containsKey("arrayValue") || keys.containsKey("dictionaryValue") ||
            keys.containsKey("constantValue") || keys.containsKey("argumentReference") ||
            keys.containsKey("functionDefinitionValue")) {
            if (sizeCount <= size) {
              map += (getArgNameByIndex(node.getFunctionName, i) -> parseValuetoString(keys))
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
        }
      }
    }
  }

  def trans(jsonObject: JSONObject, UUID: String): Unit = {
    val node: Node = new Node
    node.setFunctionName(jsonObject.getJSONObject(UUID).getJSONObject("functionInvocationValue").getString("functionName"))
    node.setArguments(jsonObject.getJSONObject(UUID).getJSONObject("functionInvocationValue").getJSONObject("arguments"))
    node.setDepth(1)
    node.setWidth(1)
    node.setUUID(UUID)

    BackAndOn(node, jsonObject)
    val arg: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]
    = mutable.ArrayBuffer.empty[(String, String, mutable.Map[String, String])]
    DFS(List(node), arg)
    dagMap += (UUID -> arg)
  }

  def clear(): Unit = {
    dagMap.clear()
  }
}