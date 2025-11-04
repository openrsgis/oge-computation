package whu.edu.cn.trigger

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.typesafe.scalalogging.StrictLogging
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
//import whu.edu.cn.algorithms.ImageProcess.algorithms_Image._
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.{AverageNearestNeighbor, DescriptiveStatistics, KernelDensityEstimation, PrincipalComponentAnalysis, RipleysK}
import whu.edu.cn.algorithms.SpatialStats.GWModels
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.{CorrelationAnalysis, SpatialAutoCorrelation, TemporalAutoCorrelation}
import whu.edu.cn.algorithms.SpatialStats.STSampling.{Sampling, SandwichSampling}
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.{IDW, Kriging, LinearInterpolation, NearestNeighbourInterpolation, SplineInterpolation}
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression._
import whu.edu.cn.algorithms.terrain.calculator
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.entity.OGEClassType.OGEClassType
import whu.edu.cn.entity._
import whu.edu.cn.entity.cube.CubeTileKey
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.oge._
import whu.edu.cn.plugin.PluginLoader
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.{ClientUtil, JedisUtil, PostSender}

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.Executors
import scala.collection.mutable.{ListBuffer, Map}
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future, TimeoutException}
import scala.io.{BufferedSource, Source}
import scala.language.postfixOps
import scala.util.Random

object Trigger extends StrictLogging with Serializable {

  /**
   * Optimazed DAG
   */
  var optimizedDagMap: mutable.Map[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]] = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]]

  /**
   * Coverage Collection Metadata
   */
  var coverageCollectionMetadata: mutable.Map[String, CoverageCollectionMetadata] = mutable.Map.empty[String, CoverageCollectionMetadata]

  /**
   *Raster array metadata list
   */

  /**
   *Delay function
   */
  var lazyFunc: mutable.Map[String, (String, mutable.Map[String, String])] = mutable.Map.empty[String, (String, mutable.Map[String, String])]

  /**
   *RDD cache list of raster collections
   */
  var coverageCollectionRddList: mutable.Map[String, immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]] = mutable.Map.empty[String, immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]]

  /**
   *Raster data RDD cache list
   */
  var coverageRddList: mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = mutable.Map.empty[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]

  /**
   *Band information cache list
   */
  var bandList: mutable.Map[String, immutable.Map[Int, Double]] = mutable.Map.empty[String, immutable.Map[Int, Double]]

  /**
   *Floating point cache list
   */
  var doubleList: mutable.Map[String, Double] = mutable.Map.empty[String, Double]

  /**
   *String cache list
   */
  var stringList: mutable.Map[String, String] = mutable.Map.empty[String, String]

  /**
   *Integer cache list
   */
  var intList: mutable.Map[String, Int] = mutable.Map.empty[String, Int]

  /**
   *Table data cache list
   */
  var coverageParamsList: mutable.Map[String, Array[(String, String)]] = mutable.Map.empty[String, Array[(String, String)]]
  var paramsMap: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = mutable.ArrayBuffer.empty

  var tableRddList: mutable.Map[String, String] = mutable.Map.empty[String, String]
  var kernelRddList: mutable.Map[String, geotrellis.raster.mapalgebra.focal.Kernel] = mutable.Map.empty[String, geotrellis.raster.mapalgebra.focal.Kernel]
  var mlmodelRddList: mutable.Map[String, org.apache.spark.ml.PipelineModel] = mutable.Map.empty[String, org.apache.spark.ml.PipelineModel]
  var featureRddList: mutable.Map[String, Any] = mutable.Map.empty[String, Any]

  val ogeGeometryList: mutable.Map[String, OGEGeometry] = mutable.Map.empty[String, OGEGeometry]
  val featureList: mutable.Map[String, Feature] = mutable.Map.empty[String, Feature]
  var featureCollectionList: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

  var cubeRDDList: mutable.Map[String, Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]] = mutable.Map.empty[String, Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]]

  var cubeLoad: mutable.Map[String, (String, String, String)] = mutable.Map.empty[String, (String, String, String)]
  var outputInformationList: mutable.ListBuffer[JSONObject] = mutable.ListBuffer.empty[JSONObject]

  /**
   *A cache list that stores the execution results of the upstream DAG nodes during all executions<br>
   *Subsequent consideration will ultimately be to migrate all cached results here instead of placing them in xxxRDDList<br>
   *Key: DAG node id<br>
   *Value: DAG node execution result<br>
   */
  private val dagResultCacheMap: mutable.Map[String, AllowedData] = mutable.Map.empty[String, AllowedData]


  var userId: String = _

  /**
   *Zoom level
   */
  var level: Int = _

  /**
   *Rendering layer name
   */
  var layerName: String = _

  /**
   *Rendering window range
   */
  var windowExtent: Extent = _

  /**
   *Whether to batch process tasks
   */
  var isBatch: Int = _

  /**
   *The task DAG JSON content of this calculation work
   */
  var workTaskJson: String = _

  var workType: String = _

  /**
   * DAG id
   */
  var dagId: String = _
  

  /**
   *z-index list
   */
  val zIndexStrArray = new mutable.ArrayBuffer[String]

  /**
   *Batch processing parameters
   */
  val batchParam: BatchParam = new BatchParam

  /**
   *Onthefly output calculation level
   */
  var ontheFlyLevel: Int = _

  /**
   *An auto-increment identifier used to identify the number for reading the uploaded file
   */
  var file_id: Long = 0

  /**
   *Temporary file list
   */
  val tempFileList = new ListBuffer[String]

  /**
   *Task source
   */
  var dagType = ""

  /**
   *Task result file name
   */
  var outputFile = ""

  /**
   *Process name
   */
  var ProcessName: String = _

  /**
   *Whether to read the raster from the uploaded file
   */
  var coverageReadFromUploadFile: Boolean = false

  // 1. Define a fixed-size thread pool to control the maximum number of concurrencies, and adjust it according to the number of driver node cores and task characteristics.

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
  val outputFilePath = new ThreadLocal[String] {
    override def initialValue(): String = ""
  }

  def isOptionalArg(args: mutable.Map[String, String], name: String): String = {
    if (args.contains(name)) {
      args(name)
    } else {
      null
    }
  }

  // The actual coverage collection loading function

  def isActioned(implicit sc: SparkContext, UUID: String, typeEnum: OGEClassType): Unit = {
    typeEnum match {
      case OGEClassType.CoverageCollection =>
        if (!coverageCollectionRddList.contains(UUID)) {
          val metadata: CoverageCollectionMetadata = coverageCollectionMetadata(UUID)
          coverageCollectionRddList += (UUID -> CoverageCollection.load(sc,
            productName = metadata.productName,
            sensorName = metadata.sensorName,
            measurementName = metadata.measurementName,
            startTime = if (metadata.startTime != null) metadata.startTime.toString else null,
            endTime = if (metadata.endTime != null) metadata.endTime.toString else null,
            extent = metadata.extent,
            crs = metadata.crs,
            level = level,
            cloudCoverMin = metadata.getCloudCoverMin(),
            cloudCoverMax = metadata.getCloudCoverMax(),
            customFiles = metadata.getCustomFiles,
            bands = metadata.getBands,
            coverageIDList = metadata.getCoverageIDList)
            )
        }
  }}

  def getValue(name: String): (String, String) = {
    if (doubleList.contains(name)) {
      (doubleList(name).toString, "double")
    } else if (intList.contains(name)) {
      (intList(name).toString, "int")
    } else if (stringList.contains(name)) {
      (stringList(name), "String")
    } else {
      val message = s"No such element: $name in this calculation"
      logger.error(message)
      throw new Exception(message)
    }
  }

  def getCoverageListFromArgs(coverageNames: String): List[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {

    val names = coverageNames.replace("[", "").replace("]", "").split(',')
    val coverages = mutable.ListBuffer.empty[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]
    for (name <- names) {
      coverages.append(coverageRddList(name.trim))
    }
    coverages.toList
  }
  def getListFromJSONString(list: String): List[Any] = {
    import scala.util.parsing.json.JSON

    val uuidList = list.replace("[", "").replace("]", "").split(',').toList
    if (ogeGeometryList.keys.toList.contains(uuidList.head)) {
      uuidList.map(g => ogeGeometryList(g.toString))
    }

    else {
      JSON.parseFull(list) match {
        case Some(data) =>
          data.asInstanceOf[List[Any]]
        case None =>
          throw new IllegalArgumentException("Invalid nested list format")
      }
    }
  }
  def getMapFromJSONString(list: String): Map[String, Any] = {
    import scala.util.parsing.json.JSON
    JSON.parseFull(list) match {
      case Some(data) =>
        data.asInstanceOf[Map[String, Any]]
      case None =>
        throw new IllegalArgumentException("Invalid nested list format")
    }
  }
  /**
   *Step execution method. The function name and parameters of each execution node in the DAG are passed into this method, and then the actual operation is performed based on the node function name.
   *
   * @param sc Spark context, implicitly passed automatically
   * @param UUID UUID of the node
   * @param funcName node function name
   * @param args node parameters
   */
  @throws(classOf[Throwable])
  def func(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    try {
      logger.info(s"funcName: $funcName args: $args")

      val function = JsonToArg.getAlgorithmJson(funcName)

      if (function.containsKey("type") && function.getInteger("type") == 2) {

      } else {
        funcName match {
          //Others
          case "Service.printString" =>
            val temp = getValue(args("object"))
            val res = temp._1
            val valueType = temp._2
            Service.print(res, args("name"), valueType)
          case "Service.printNumber" =>
            val temp = getValue(args("object"))
            val res = temp._1
            val valueType = temp._2
            Service.print(res, args("name"), valueType)
          case "Service.printList" =>
            val temp = getValue(args("object"))
            val res = temp._1
            val valueType = temp._2
            Service.print(res, args("name"), valueType)
          case "Service.getCoverageCollection" =>
            lazyFunc += (UUID -> (funcName, args))
            coverageCollectionMetadata += (UUID -> Service.getCoverageCollection(
              isOptionalArg(args, "productID"),
              dateTime = isOptionalArg(args, "datetime"),
              extent = isOptionalArg(args, "bbox"),
              cloudCoverMin = if (isOptionalArg(args, "cloudCoverMin") == null) 0 else isOptionalArg(args, "cloudCoverMin").toFloat,
              cloudCoverMax = if (isOptionalArg(args, "cloudCoverMax") == null) 100 else isOptionalArg(args, "cloudCoverMax").toFloat,
              customFiles = isOptionalArg(args, "customFiles"),
              bands = isOptionalArg(args, "bands"),
              coverageIDList = isOptionalArg(args, "coverageIDList")
            )
              )

          case "Service.getCoverageByFeature" =>
            lazyFunc += (UUID -> (funcName, args))
          case "Service.getCoverage" =>
            if (args("coverageID").contains("file:")) {
              coverageReadFromUploadFile = true
              coverageRddList += (UUID -> Coverage.loadCoverageFromUpload(sc, args("coverageID"), userId, dagId))
            } else if (args("coverageID").startsWith("myData/") || args("coverageID").startsWith("result/")) {
              coverageReadFromUploadFile = true
              coverageRddList += (UUID -> Coverage.loadCoverageFromUpload(sc, args("coverageID"), userId, dagId))
            } else if (args("coverageID").startsWith("OGE_Case_Data/")) {
              coverageReadFromUploadFile = true
              coverageRddList += (UUID -> Coverage.loadCoverageFromCaseData(sc, args("coverageID"), dagId))
            }
            else {
              coverageReadFromUploadFile = false
              coverageRddList += (UUID -> Service.getCoverage(sc, args("coverageID"), args("productID"), level = level))
            }
          case "Service.getCube" =>
            cubeRDDList += (UUID -> Service.getCube(sc, args("cubeId"), args("products"), args("bands"), args("time"), args("extent"), args("tms"), args("resolution")))
          case "Service.getTable" =>
            tableRddList += (UUID -> isOptionalArg(args, "productID"))
          case "Service.getFeatureCollection" =>
            if (args("productId").startsWith("myData/") || args("productId").startsWith("result/")) {
              featureCollectionList += (UUID -> FeatureCollection.loadFeatureCollectionFromUpload(sc, args("productId"), userId, dagId, if(!args.contains("crs")) "EPSG:4326" else args("crs")))
            } else {
              featureCollectionList += (UUID -> FeatureCollection.load(sc, productId = args("productId"), extent = if(!args.contains("extent")) null else ogeGeometryList(args("extent")), startTime = isOptionalArg(args,"startTime"), endTime = isOptionalArg(args,"endTime"), crs = if(!args.contains("crs")) null else args("crs")))
            }
          case "Service.getFeature" =>
            if (args.contains("productId")) 
              featureList += (UUID -> Feature.load(args("featureId"), args("productId"), if (args.contains("crs")) args("crs") else "EPSG:4326"))
            else //从用户目录获取Feature
              featureList += (UUID -> Feature.loadFeatureFromUpload(args("featureId"), userId, dagId, if (args.contains("crs")) args("crs") else "EPSG:4326"))
          case "Filter.equals" =>
            lazyFunc += (UUID -> (funcName, args))
          case "Filter.and" =>
            lazyFunc += (UUID -> (funcName, args))
          case "Filter.date" =>
            lazyFunc += (UUID -> (funcName, args))
          case "Filter.bounds" =>
            lazyFunc += (UUID -> (funcName, args))


          // CoverageCollection
          case "CoverageCollection.filter" =>
            coverageCollectionMetadata += (UUID -> CoverageCollection.filter(filter = args("filter"), collection = coverageCollectionMetadata(args("collection"))))
          case "CoverageCollection.mergeCoverages" =>
            coverageCollectionRddList += (UUID -> CoverageCollection.mergeCoverages(getCoverageListFromArgs(args("coverages")), args("names").replace("[", "").replace("]", "").split(',').toList))
          case "CoverageCollection.mosaic" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID ->
              CoverageCollection.mosaic(sc, coverageCollectionRddList(args("coverageCollection")), isOptionalArg(args, "method")))
          case "CoverageCollection.mean" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.mean(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.min" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.min(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.max" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.max(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.sum" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.sum(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.or" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.or(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.and" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.and(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.median" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.median(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.mode" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.mode(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.cat" =>
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            coverageRddList += (UUID -> CoverageCollection.cat(coverageCollectionRddList(args("coverageCollection"))))
          case "CoverageCollection.addStyles" =>
            if (isBatch == 0) {
              isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
              val visParam: VisualizationParam = new VisualizationParam
              visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
              CoverageCollection.visualizeOnTheFly(sc, coverageCollection = coverageCollectionRddList(args("coverageCollection")), visParam = visParam)
            }
            else {
              CoverageCollection.visualizeBatch(sc, coverageCollection = coverageCollectionRddList(args("coverageCollection")))
            }





          // Coverage
          case "Coverage.export" =>
            if (outputFilePath.get().nonEmpty) {
              logger.warn("文件自动上传跳过转换 {}", outputFilePath.get())
              val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
              val path = if (outputFilePath.get().endsWith(batchParam.getFormat)) {
                batchParam.getUserId + "/result/" + batchParam.getFileName + UUID.toString + "." + batchParam.getFormat
              } else {
                batchParam.getUserId + "/result/" + batchParam.getFileName + outputFilePath.get()
              }
              try {
                clientUtil.Upload(path, outputFilePath.get())
              } catch {
                case e: Exception =>
                  logger.error(s"Upload failed, outputFilePath ${outputFilePath.get()}  path $path,{}", e.getMessage)
              }
              val jedis = JedisUtil.getJedis
              val json = jedis.get(s"oge:result:${batchParam.getUserId}:${dagId}:")
              val jsonArray: JSONArray = JSON.parseArray(Option(json).getOrElse("[]"));
              jsonArray.add(new JSONObject().fluentPut("path", path))
              jedis.set(s"oge:result:${batchParam.getUserId}:${dagId}:", jsonArray.toString)
              logger.info(s"Start output executor ${jsonArray} path ${path}")
              JedisUtil.returnResource(jedis)

            } else {
              Coverage.visualizeBatch(sc, coverage = coverageRddList(args("coverage")), batchParam = batchParam, dagId)
            }


          case "Coverage.area" =>
            doubleList += (UUID -> Coverage.area(coverage = coverageRddList(args("coverage")), ValList = args("valueRange").substring(1, args("valueRange").length - 1).split(",").toList, resolution = args("resolution").toDouble))
          case "Coverage.geoDetector" =>
            stringList += (UUID -> Coverage.geoDetector(depVar_In = coverageRddList(args("depVar_In")),
              facVar_name_In = args("facVar_name_In"),
              facVar_In = coverageRddList(args("facVar_In")),
              norExtent_sta = args("norExtent_sta").toDouble, norExtent_end = args("norExtent_end").toDouble, NaN_value = args("NaN_value").toDouble))
          case "Coverage.rasterUnion" =>
            coverageRddList += (UUID -> Coverage.rasterUnion(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.reclass" =>

            val pattern = "\\],\\["
            val subStrings = args("rules").stripPrefix("[").stripSuffix("]").split(pattern)

            val rules_res = subStrings.map { subString =>
              val elements = subString.split(",").map { element =>
                element.stripPrefix("[").stripSuffix("]").toDouble
              }
              elements.toList
            }.map {
              case List(a, b, c) => (a, b, c)
            }.toList
            coverageRddList += (UUID -> Coverage.reclass(coverage = coverageRddList(args("coverage")),
              rules = rules_res,
              NaN_value = args("NaN_value").toDouble))
          case "Coverage.date" =>
            stringList += (UUID -> Coverage.date(coverage = coverageRddList(args("coverage"))))
          case "Coverage.subtract" =>
            coverageRddList += (UUID -> Coverage.subtract(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.subtractNum" =>
            coverageRddList += (UUID -> Coverage.subtractNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
          case "Coverage.cat" =>
            coverageRddList += (UUID -> Coverage.cat(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.add" =>
            coverageRddList += (UUID -> Coverage.add(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.addNum" =>
            coverageRddList += (UUID -> Coverage.addNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
          case "Coverage.mod" =>
            coverageRddList += (UUID -> Coverage.mod(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.modNum" =>
            coverageRddList += (UUID -> Coverage.modNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
          case "Coverage.divide" =>
            coverageRddList += (UUID -> Coverage.divide(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.divideNum" =>
            coverageRddList += (UUID -> Coverage.divideNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
          case "Coverage.round" =>
            coverageRddList += (UUID -> Coverage.round(coverage = coverageRddList(args("coverage"))))
          case "Coverage.multiply" =>
            coverageRddList += (UUID -> Coverage.multiply(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.multiplyNum" =>
            coverageRddList += (UUID -> Coverage.multiplyNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
          case "Coverage.normalizedDifference" =>
            coverageRddList += (UUID -> Coverage.normalizedDifference(coverageRddList(args("coverage")), bandNames = args("bandNames").substring(1, args("bandNames").length - 1).split(",").toList))
          case "Coverage.binarization" =>
            coverageRddList += (UUID -> Coverage.binarization(coverage = coverageRddList(args("coverage")), threshold = args("threshold").toDouble))
          case "Coverage.and" =>
            coverageRddList += (UUID -> Coverage.and(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.or" =>
            coverageRddList += (UUID -> Coverage.or(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.not" =>
            coverageRddList += (UUID -> Coverage.not(coverage = coverageRddList(args("coverage1"))))
          case "Coverage.bitwiseAnd" =>
            coverageRddList += (UUID -> Coverage.bitwiseAnd(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.bitwiseXor" =>
            coverageRddList += (UUID -> Coverage.bitwiseXor(coverage1 = coverageRddList(args("coverage1")), coverage2 =
              coverageRddList(args("coverage2"))))
          case "Coverage.bitwiseOr" =>
            coverageRddList += (UUID -> Coverage.bitwiseOr(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.bitwiseNot" =>
            coverageRddList += (UUID -> Coverage.bitwiseNot(coverage = coverageRddList(args("coverage"))))
          case "Coverage.ceil" =>
            coverageRddList += (UUID -> Coverage.ceil(coverage = coverageRddList(args("coverage"))))
          case "Coverage.floor" =>
            coverageRddList += (UUID -> Coverage.floor(coverage = coverageRddList(args("coverage"))))
          case "Coverage.sin" =>
            coverageRddList += (UUID -> Coverage.sin(coverage = coverageRddList(args("coverage"))))
          case "Coverage.tan" =>
            coverageRddList += (UUID -> Coverage.tan(coverage = coverageRddList(args("coverage"))))
          case "Coverage.tanh" =>
            coverageRddList += (UUID -> Coverage.tanh(coverage = coverageRddList(args("coverage"))))
          case "Coverage.cos" =>
            coverageRddList += (UUID -> Coverage.cos(coverage = coverageRddList(args("coverage"))))
          case "Coverage.sinh" =>
            coverageRddList += (UUID -> Coverage.sinh(coverage = coverageRddList(args("coverage"))))
          case "Coverage.cosh" =>
            coverageRddList += (UUID -> Coverage.cosh(coverage = coverageRddList(args("coverage"))))
          case "Coverage.asin" =>
            coverageRddList += (UUID -> Coverage.asin(coverage = coverageRddList(args("coverage"))))
          case "Coverage.acos" =>
            coverageRddList += (UUID -> Coverage.acos(coverage = coverageRddList(args("coverage"))))
          case "Coverage.atan" =>
            coverageRddList += (UUID -> Coverage.atan(coverage = coverageRddList(args("coverage"))))
          case "Coverage.atan2" =>
            coverageRddList += (UUID -> Coverage.atan2(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.eq" =>
            coverageRddList += (UUID -> Coverage.eq(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.gt" =>
            coverageRddList += (UUID -> Coverage.gt(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.gte" =>
            coverageRddList += (UUID -> Coverage.gte(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.lt" =>
            coverageRddList += (UUID -> Coverage.lt(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.lte" =>
            coverageRddList += (UUID -> Coverage.lte(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.log" =>
            coverageRddList += (UUID -> Coverage.log(coverage = coverageRddList(args("coverage"))))
          case "Coverage.log10" =>
            coverageRddList += (UUID -> Coverage.log10(coverage = coverageRddList(args("coverage"))))
          case "Coverage.selectBands" =>
            coverageRddList += (UUID -> Coverage.selectBands(coverage = coverageRddList(args("coverage")), bands = args("bands").substring(1, args("bands").length - 1).split(",").toList))
          case "Coverage.addBands" =>
            val names: List[String] = args("names").split(",").toList
            coverageRddList += (UUID -> Coverage.addBands(dstCoverage = coverageRddList(args("coverage1")), srcCoverage =
              coverageRddList(args("coverage2")), names = names))
          case "Coverage.bandNames" =>
            stringList += (UUID -> Coverage.bandNames(coverage = coverageRddList(args("coverage"))))
          case "Coverage.bandNum" =>
            intList += (UUID -> Coverage.bandNum(coverage = coverageRddList(args("coverage"))))
          case "Coverage.abs" =>
            coverageRddList += (UUID -> Coverage.abs(coverage = coverageRddList(args("coverage"))))
          case "Coverage.neq" =>
            coverageRddList += (UUID -> Coverage.neq(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.signum" =>
            coverageRddList += (UUID -> Coverage.signum(coverage = coverageRddList(args("coverage"))))
          case "Coverage.bandTypes" =>
            stringList += (UUID -> Coverage.bandTypes(coverage = coverageRddList(args("coverage"))))
          case "Coverage.rename" =>
            coverageRddList += (UUID -> Coverage.rename(coverage = coverageRddList(args("coverage")), name = args("name").split(",").toList))
          case "Coverage.pow" =>
            coverageRddList += (UUID -> Coverage.pow(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.powNum" =>
            coverageRddList += (UUID -> Coverage.powNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
          case "Coverage.mini" =>
            coverageRddList += (UUID -> Coverage.mini(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.maxi" =>
            coverageRddList += (UUID -> Coverage.maxi(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
          case "Coverage.focalMean" =>
            coverageRddList += (UUID -> Coverage.focalMean(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
          case "Coverage.focalMedian" =>
            coverageRddList += (UUID -> Coverage.focalMedian(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
          case "Coverage.focalMin" =>
            coverageRddList += (UUID -> Coverage.focalMin(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
          case "Coverage.focalMax" =>
            coverageRddList += (UUID -> Coverage.focalMax(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
          case "Coverage.focalMode" =>
            coverageRddList += (UUID -> Coverage.focalMode(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
          case "Coverage.convolve" =>
            coverageRddList += (UUID -> Coverage.convolve(coverage = coverageRddList(args("coverage")), kernel = kernelRddList(args("kernel"))))
          case "Coverage.projection" =>
            stringList += (UUID -> Coverage.projection(coverage = coverageRddList(args("coverage"))))
          case "Coverage.remap" =>
            coverageRddList += (UUID -> Coverage.remap(coverage = coverageRddList(args("coverage")), args("from").slice(1, args("from").length - 1).split(',').toList.map(_.toInt),
              args("to").slice(1, args("to").length - 1).split(',').toList.map(_.toDouble), isOptionalArg(args, "defaultValue")))
          case "Coverage.polynomial" =>
            coverageRddList += (UUID -> Coverage.polynomial(coverage = coverageRddList(args("coverage")), args("l").slice(1, args("l").length - 1).split(',').toList.map(_.toDouble)))
          case "Coverage.slice" =>
            coverageRddList += (UUID -> Coverage.slice(coverage = coverageRddList(args("coverage")), start = args("start").toInt, end = args("end").toInt))
          case "Coverage.histogram" =>
            stringList += (UUID -> Coverage.histogram(coverage = coverageRddList(args("coverage")), scale = args("scale").toDouble).toString())
          case "Coverage.reproject" =>
            coverageRddList += (UUID -> Coverage.reproject(coverage = coverageRddList(args("coverage")), crs = CRS.fromEpsgCode(args("crsCode").toInt), scale = args("resolution").toDouble))
          case "Coverage.resample" =>
            coverageRddList += (UUID -> Coverage.resample(coverage = coverageRddList(args("coverage")), level = args("level").toDouble, mode = args("mode")))
          case "Coverage.gradient" =>
            coverageRddList += (UUID -> Coverage.gradient(coverage = coverageRddList(args("coverage"))))
          case "Coverage.clip" =>
            coverageRddList += (UUID -> Coverage.clip(coverage = coverageRddList(args("coverage")), geom = featureRddList(args("geom")).asInstanceOf[Geometry]))
          case "Coverage.clamp" =>
            coverageRddList += (UUID -> Coverage.clamp(coverage = coverageRddList(args("coverage")), low = args("low").toInt, high = args("high").toInt))
          case "Coverage.rgbToHsv" =>
            coverageRddList += (UUID -> Coverage.rgbToHsv(coverage = coverageRddList(args("coverage"))))
          case "Coverage.hsvToRgb" =>
            coverageRddList += (UUID -> Coverage.hsvToRgb(coverage = coverageRddList(args("coverage"))))
          case "Coverage.entropy" =>
            coverageRddList += (UUID -> Coverage.entropy(coverage = coverageRddList(args("coverage")), "square", radius = args("radius").toInt))
          //      case "Coverage.NDVI" =>
          //        coverageRddList += (UUID -> Coverage.NDVI(NIR = coverageRddList(args("NIR")), Red = coverageRddList(args("Red"))))
          case "Coverage.cbrt" =>
            coverageRddList += (UUID -> Coverage.cbrt(coverage = coverageRddList(args("coverage"))))
          case "Coverage.sqrt" =>
            coverageRddList += (UUID -> Coverage.sqrt(coverage = coverageRddList(args("coverage"))))
          case "Coverage.metadata" =>
            stringList += (UUID -> Coverage.metadata(coverage = coverageRddList(args("coverage"))))
          case "Coverage.mask" =>
            coverageRddList += (UUID -> Coverage.mask(coverageRddList(args("coverage1")), coverageRddList(args("coverage2")), args("readMask").toInt, args("writeMask").toInt))
          case "Coverage.toInt8" =>
            coverageRddList += (UUID -> Coverage.toInt8(coverage = coverageRddList(args("coverage"))))
          case "Coverage.toUint8" =>
            coverageRddList += (UUID -> Coverage.toUint8(coverage = coverageRddList(args("coverage"))))
          case "Coverage.toInt16" =>
            coverageRddList += (UUID -> Coverage.toInt16(coverage = coverageRddList(args("coverage"))))
          case "Coverage.toUint16" =>
            coverageRddList += (UUID -> Coverage.toUint16(coverage = coverageRddList(args("coverage"))))
          case "Coverage.toInt32" =>
            coverageRddList += (UUID -> Coverage.toInt32(coverage = coverageRddList(args("coverage"))))
          case "Coverage.toFloat" =>
            coverageRddList += (UUID -> Coverage.toFloat(coverage = coverageRddList(args("coverage"))))
          case "Coverage.toDouble" =>
            coverageRddList += (UUID -> Coverage.toDouble(coverage = coverageRddList(args("coverage"))))
          case "Coverage.gteByGEE" =>
            coverageRddList += (UUID -> Coverage.gteByGEE(coverage = coverageRddList(args("coverage")), threshold = args("threshold").toDouble))
          case "Coverage.lteByGEE" =>
            coverageRddList += (UUID -> Coverage.lteByGEE(coverage = coverageRddList(args("coverage")), threshold = args("threshold").toDouble))
          case "Coverage.updateMask" =>
            coverageRddList += (UUID -> Coverage.updateMask(coverageRddList(args("coverage1")), coverageRddList(args("coverage2"))))
          case "Coverage.unmask" =>
            coverageRddList += (UUID -> Coverage.unmask(coverageRddList(args("coverage1")), coverageRddList(args("coverage2"))))
          case "Coverage.setValidDataRange" =>
            coverageRddList += (UUID -> Coverage.setValidDataRange(coverage = coverageRddList(args("coverage")), args("range").slice(1, args("range").length - 1).split(',').toList.map(_.toDouble)))
          case "Coverage.setValueRangeByPercentage" =>
            coverageRddList += (UUID -> Coverage.setValueRangeByPercentage(coverage = coverageRddList(args("coverage")), args("minimum").toDouble, args("maximum").toDouble))



          // Kernel
          case "Kernel.chebyshev" =>
            kernelRddList += (UUID -> Kernel.chebyshev(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.circle" =>
            kernelRddList += (UUID -> Kernel.circle(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.compass" =>
            kernelRddList += (UUID -> Kernel.compass(args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.diamond" =>
            kernelRddList += (UUID -> Kernel.diamond(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.euclidean" =>
            kernelRddList += (UUID -> Kernel.euclidean(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.fixed" =>
            val kernel = Kernel.fixed(weights = args("weights"))
            kernelRddList += (UUID -> kernel)
          case "Kernel.gaussian" =>
            kernelRddList += (UUID -> Kernel.gaussian(args("radius").toInt, args("sigma").toFloat, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.inverse" =>
            kernelRddList += (UUID -> Kernel.inverse(kernelRddList(args("kernel"))))
          case "Kernel.manhattan" =>
            kernelRddList += (UUID -> Kernel.manhattan(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.octagon" =>
            kernelRddList += (UUID -> Kernel.octagon(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.plus" =>
            kernelRddList += (UUID -> Kernel.plus(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.square" =>
            val kernel = Kernel.square(radius = args("radius").toInt, normalize = args("normalize").toBoolean, value = args("value").toDouble)
            kernelRddList += (UUID -> kernel)
            print(kernel.tile.asciiDraw())
          case "Kernel.rectangle" =>
            kernelRddList += (UUID -> Kernel.rectangle(args("xRadius").toInt, args("yRadius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.roberts" =>
            kernelRddList += (UUID -> Kernel.roberts(args("normalize").toBoolean, args("magnitude").toFloat))
          case "Kernel.rotate" =>
            kernelRddList += (UUID -> Kernel.rotate(kernelRddList(args("kernel")), args("rotations").toInt))
          case "Kernel.prewitt" =>
            val kernel = Kernel.prewitt(args("normalize").toBoolean, args("magnitude").toFloat)
            kernelRddList += (UUID -> kernel)
            print(kernel.tile.asciiDraw())
          case "Kernel.kirsch" =>
            val kernel = Kernel.kirsch(args("normalize").toBoolean, args("magnitude").toFloat)
            kernelRddList += (UUID -> kernel)
          case "Kernel.sobel" =>
            val kernel = Kernel.sobel(axis = args("axis"))
            kernelRddList += (UUID -> kernel)
          case "Kernel.laplacian4" =>
            val kernel = Kernel.laplacian4()
            kernelRddList += (UUID -> kernel)
          case "Kernel.laplacian8" =>
            val kernel = Kernel.laplacian8()
            kernelRddList += (UUID -> kernel)
          case "Kernel.laplacian8" =>
            val kernel = Kernel.laplacian8()
            kernelRddList += (UUID -> kernel)
            print(kernel.tile.asciiDraw())
          case "Kernel.add" =>
            val kernel = Kernel.add(kernel1 = kernelRddList(args("kernel1")), kernel2 = kernelRddList(args("kernel2")))
            kernelRddList += (UUID -> kernel)

          // Terrain
          case "Coverage.slope" =>
            coverageRddList += (UUID -> Coverage.slope(coverage = coverageRddList(args("coverage")), radius = args("radius").toDouble.toInt, zFactor = args("Z_factor").toDouble))
          case "Coverage.aspect" =>
            coverageRddList += (UUID -> Coverage.aspect(coverage = coverageRddList(args("coverage")), radius = args("radius").toInt))

          // Terrain By CYM
          case "Coverage.terrSlope" =>
            coverageRddList += (UUID -> calculator.Slope(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrAspect" =>
            coverageRddList += (UUID -> calculator.Aspect(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrRuggedness" =>
            coverageRddList += (UUID -> calculator.Ruggedness(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrSlopelength" =>
            coverageRddList += (UUID -> calculator.SlopeLength(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrCurvature" =>
            coverageRddList += (UUID -> calculator.Curvature(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrHillshade" =>
            coverageRddList += (UUID -> calculator.HillShade(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrPitrouter" =>
            coverageRddList += (UUID -> calculator.PitRouter(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrPiteliminator" =>
            coverageRddList += (UUID -> calculator.PitEliminator(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrFlowdirection" =>
            coverageRddList += (UUID -> calculator.FlowDirection(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrFlowaccumulation" =>
            coverageRddList += (UUID -> calculator.FlowAccumulation(rddImage = coverageRddList(args("coverage")), zFactor = args("z-Factor").toDouble))
          case "Coverage.terrChannelnetwork" =>
            featureRddList += (UUID -> calculator.ChannelNetwork(rddImage = coverageRddList(args("DEM")), flowAccumulationImage = coverageRddList(args("FlowAccumulation")), dirImage = coverageRddList(args("FlowDirection")), zFactor = args("z-Factor").toDouble, threshold = args("threshold").toDouble))
          case "Coverage.terrFilter" =>
            coverageRddList += (UUID -> calculator.Filter(rddImage = coverageRddList(args("coverage")), min = args("min").slice(1, args("min").length - 1).split(',').toList.map(_.toDouble), max = args("max").slice(1, args("max").length - 1).split(',').toList.map(_.toDouble), zFactor = args("z-Factor").toDouble))
          case "Coverage.terrStrahlerOrder" =>
            coverageRddList += (UUID -> calculator.StrahlerOrder(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrFlowConnectivity" =>
            coverageRddList += (UUID -> calculator.FlowConnectivity(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrFlowLength" =>
            coverageRddList += (UUID -> calculator.FlowLength(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrFlowWidth" =>
            coverageRddList += (UUID -> calculator.FlowWidth(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
          case "Coverage.terrWatershedBasins" =>
            coverageRddList += (UUID -> calculator.WatershedBasins(rddImage = coverageRddList(args("coverage")), flowAccumulationImage = coverageRddList(args("FlowAccumulation")), zFactor = args("z-Factor").toDouble))
          case "Coverage.terrFeatureSelect" =>
            featureRddList += (UUID -> calculator.FeatureSelect(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble, vipValue = args("vipValue").toDouble))
          case "Coverage.terrTIN" =>
            featureRddList += (UUID -> calculator.TIN(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble, vipValue = args("vipValue").toDouble, geometryType = args("geometryType").toInt))


          case "Coverage.addStyles" =>
            val visParam: VisualizationParam = new VisualizationParam
            visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
            println("isBatch", isBatch)
            if (isBatch == 0) {
              if (workType.equals("main")) {
                Coverage.visualizeOnTheFly(sc, coverage = coverageRddList(args("coverage")), visParam = visParam)
              } else if (workType.equals("edu")) {
                Coverage.visualizeBatch_edu(sc, coverage = coverageRddList(args("coverage")), batchParam = batchParam, dagId)
              }

            } else {
              coverageRddList += (UUID -> Coverage.addStyles(coverageRddList(args("coverage")), visParam = visParam))
            }
          // thirdAlgorithm

          //Feature_deprecated
          case "Feature_deprecated.load" =>
            if (args.contains("featureId")) {
              if (args("featureId").startsWith("myData/")) {
                if (args.contains("crs"))
                  featureRddList += (UUID -> Feature_deprecated.loadFeatureFromUpload(sc, args("featureId"), userId, dagId, isOptionalArg(args, "crs")))
                else
                  featureRddList += (UUID -> Feature_deprecated.loadFeatureFromUpload(sc, args("featureId"), userId, dagId, "EPSG:4326"))
              } else {
                featureRddList += (UUID -> Feature_deprecated.load(sc, args("featureId"), isOptionalArg(args, "dataTime"), isOptionalArg(args, "crs")))
              }
              return
            }
            var dateTime = isOptionalArg(args, "dateTime")
            if (dateTime == "null")
              dateTime = null
            println("dateTime:" + dateTime)
            if (dateTime != null) {
              if (isOptionalArg(args, "crs") != null)
                featureRddList += (UUID -> Feature_deprecated.load(sc, args("productName"), args("dateTime"), args("crs")))
              else
                featureRddList += (UUID -> Feature_deprecated.load(sc, args("productName"), args("dateTime")))
            }
            else
              featureRddList += (UUID -> Feature_deprecated.load(sc, args("productName")))
          case "Feature_deprecated.point" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.point(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.point(sc, args("coors"), args("properties")))
          case "Feature_deprecated.lineString" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.lineString(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.lineString(sc, args("coors"), args("properties")))
          case "Feature_deprecated.linearRing" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.linearRing(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.linearRing(sc, args("coors"), args("properties")))
          case "Feature_deprecated.polygon" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.polygon(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.polygon(sc, args("coors"), args("properties")))
          case "Feature_deprecated.multiPoint" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.multiPoint(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.multiPoint(sc, args("coors"), args("properties")))
          case "Feature_deprecated.multiLineString" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.multiLineString(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.multiLineString(sc, args("coors"), args("properties")))
          case "Feature_deprecated.multiPolygon" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.multiPolygon(sc, args("coors"), args("properties"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.multiPolygon(sc, args("coors"), args("properties")))
          case "Feature_deprecated.geometry" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.geometry(sc, args("coors"), args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.geometry(sc, args("coors")))
          case "Feature_deprecated.area" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.area(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.area(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.bounds" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.bounds(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.bounds(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.centroid" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.centroid(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.centroid(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.buffer" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.buffer(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble, args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.buffer(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble))
          case "Feature_deprecated.convexHull" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.convexHull(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.convexHull(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.coordinates" =>
            stringList += (UUID -> Feature_deprecated.coordinates(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.reproject" =>
            featureRddList += (UUID -> Feature_deprecated.reproject(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("tarCrsCode")))
          case "Feature_deprecated.isUnbounded" =>
            stringList += (UUID -> Feature_deprecated.isUnbounded(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.getType" =>
            stringList += (UUID -> Feature_deprecated.getType(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.projection" =>
            stringList += (UUID -> Feature_deprecated.projection(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.toGeoJSONString" =>
            stringList += (UUID -> Feature_deprecated.toGeoJSONString(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.length" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.length(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.length(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.geometries" =>
            featureRddList += (UUID -> Feature_deprecated.geometries(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.dissolve" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.dissolve(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.dissolve(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.contains" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.contains(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.contains(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.containedIn" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.containedIn(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.containedIn(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.disjoint" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.disjoint(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.disjoint(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.distance" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.distance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.distance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.difference" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.difference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.difference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.intersection" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.intersection(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.intersection(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.intersects" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.intersects(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.intersects(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.symmetricDifference" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.symmetricDifference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.symmetricDifference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.union" =>
            if (isOptionalArg(args, "crs") != null)
              featureRddList += (UUID -> Feature_deprecated.union(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
            else
              featureRddList += (UUID -> Feature_deprecated.union(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.withDistance" =>
            if (isOptionalArg(args, "crs") != null)
              stringList += (UUID -> Feature_deprecated.withDistance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble, args("crs")))
            else
              stringList += (UUID -> Feature_deprecated.withDistance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
                featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble))
          case "Feature_deprecated.copyProperties" =>
            val propertyList: List[String] =
              if (args("properties") == "[]") {
                Nil 
              } else {
                args("properties").replace("[", "").replace("]", "").replace("\"", "").split(",").toList
              }
            featureRddList += (UUID -> Feature_deprecated.copyProperties(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], propertyList))
          case "Feature_deprecated.get" =>
            stringList += (UUID -> Feature_deprecated.get(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")).toString())
          case "Feature_deprecated.propertyNames" =>
            stringList += (UUID -> Feature_deprecated.propertyNames(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.set" =>
            featureRddList += (UUID -> Feature_deprecated.set(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
          case "Feature_deprecated.setGeometry" =>
            featureRddList += (UUID -> Feature_deprecated.setGeometry(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("geom")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
          case "Feature_deprecated.featureCollection" =>
            featureRddList += (UUID -> Feature_deprecated.featureCollection(sc, args("featureList").stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList.map(t => {
              featureRddList(t).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]
            })))
          case "Feature_deprecated.addStyles" =>
            Feature_deprecated.visualize(feature = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], color = args("color").replace("[", "").replace("]", "").split(',').toList, attribute = args("attribute"))


          case "Cube.addStyles" => {
            val visParam: VisualizationParam = new VisualizationParam
            visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
            Cube.visualizeOnTheFly(sc, cubeRDDList(args("cube")), visParam)
          }
          case "Cube.NDVI" =>
            cubeRDDList += (UUID -> Cube.normalizedDifference(sc, cubeRDDList(args("input")), bandName1 = args("bandName1"), platform1 = args("platform1"), bandName2 = args("bandName2"), platform2 = args("platform2")))
          case "Cube.add" =>
            cubeRDDList += (UUID -> Cube.add(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
          case "Cube.subtract" =>
            cubeRDDList += (UUID -> Cube.subtract(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
          case "Cube.multiply" =>
            cubeRDDList += (UUID -> Cube.multiply(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
          case "Cube.divide" =>
            cubeRDDList += (UUID -> Cube.divide(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
          case "Coverage.filter" =>
            coverageRddList += (UUID -> Coverage.filter(coverage = coverageRddList(args("coverage")), args("min").toDouble, args("max").toDouble))

          case _ =>
            func1(sc, UUID, funcName, args)
        }
      }
    } catch {
      case e: Throwable =>

        Trigger.optimizedDagMap.clear()
        Trigger.coverageCollectionMetadata.clear()
        Trigger.lazyFunc.clear()
        Trigger.coverageCollectionRddList.clear()
        Trigger.coverageRddList.clear()
        Trigger.zIndexStrArray.clear()
        JsonToArg.dagMap.clear()

        Trigger.tableRddList.clear()
        Trigger.kernelRddList.clear()
        Trigger.mlmodelRddList.clear()
        Trigger.featureRddList.clear()
        Trigger.cubeRDDList.clear()
        Trigger.cubeLoad.clear()
        logger.error(s"Exception occurred, UUID=$UUID, functionName=$funcName, args=$args", e)
        throw e
    }
  }

  def func1(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    try {
      val tempNoticeJson = new JSONObject
      logger.info(s"funcName: $funcName, args : $args")
      funcName match {
      case "SpatialStats.BasicStatistics.AverageNearestNeighbor" =>
        stringList += (UUID -> AverageNearestNeighbor.result(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "SpatialStats.BasicStatistics.DescriptiveStatistics" =>
        stringList += (UUID -> DescriptiveStatistics.result(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "SpatialStats.BasicStatistics.PrincipalComponentAnalysis" =>
        stringList += (UUID -> PrincipalComponentAnalysis.PCA(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("properties"), args("keep").toInt, args("split"), args("is_scale").toBoolean))
      case "SpatialStats.BasicStatistics.RipleysK" =>
        stringList += (UUID -> RipleysK.ripley(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "SpatialStats.BasicStatistics.KernelDensityEstimation" =>
        coverageRddList += (UUID -> KernelDensityEstimation.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName").asInstanceOf[Option[String]], args("rows").toInt, args("cols").toInt, args("kernel"), args("size").toInt, args("sigma").toDouble, args("amplitude").toDouble, args("radius").toInt)) // KDE
      case "SpatialStats.STCorrelations.CorrelationAnalysis.corrMat" =>
        stringList += (UUID -> CorrelationAnalysis.corrMat(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("properties"), args("method")))
      case "SpatialStats.STCorrelations.SpatialAutoCorrelation.globalMoranI" =>
        stringList += (UUID -> SpatialAutoCorrelation.globalMoranI(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("plot").toBoolean, args("test").toBoolean, args("weightstyle")))
      case "SpatialStats.STCorrelations.SpatialAutoCorrelation.localMoranI" =>
        featureRddList += (UUID -> SpatialAutoCorrelation.localMoranI(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("adjust").toBoolean))
      case "SpatialStats.STCorrelations.SpatialAutoCorrelation.globalGearyC" =>
        stringList += (UUID -> SpatialAutoCorrelation.globalGearyC(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("test").toBoolean, args("weightstyle"), args("knn").toInt))
      case "SpatialStats.STCorrelations.SpatialAutoCorrelation.localGearyC" =>
        featureRddList += (UUID -> SpatialAutoCorrelation.localGearyC(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("adjust").toBoolean, args("weightstyle"), args("knn").toInt, args("nsim").toInt))
      case "SpatialStats.STCorrelations.SpatialAutoCorrelation.getisOrdG" =>
        featureRddList += (UUID -> SpatialAutoCorrelation.getisOrdG(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("weightstyle"), args("knn").toInt, args("star").toBoolean))
      case "SpatialStats.STCorrelations.TemporalAutoCorrelation.ACF" =>
        stringList += (UUID -> TemporalAutoCorrelation.ACF(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("timelag").toInt))
      case "SpatialStats.SpatialRegression.SpatialLagModel.fit" =>
        val re_slm = SpatialLagModel.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"))
        featureRddList += (UUID -> re_slm)
      case "SpatialStats.SpatialRegression.SpatialErrorModel.fit" =>
        val re_sem = SpatialErrorModel.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"))
        featureRddList += (UUID -> re_sem)
      case "SpatialStats.SpatialRegression.SpatialDurbinModel.fit" =>
        val re_sdm = SpatialDurbinModel.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"))
        featureRddList += (UUID -> re_sdm)
      case "SpatialStats.SpatialRegression.LinearRegression.feature" =>
        featureRddList += (UUID -> LinearRegression.fit(sc, featureRddList(args("data")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y"), args("x"), args("Intercept").toBoolean))
      case "SpatialStats.SpatialRegression.LogisticRegression.feature" =>
        featureRddList += (UUID -> LogisticRegression.fit(sc, featureRddList(args("data")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y"), args("x"), args("Intercept").toBoolean, args("maxIter").toInt, args("epsilon").toDouble))
      case "SpatialStats.SpatialRegression.PoissonRegression.feature" =>
        featureRddList += (UUID -> PoissonRegression.fit(sc, featureRddList(args("data")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y"), args("x"), args("Intercept").toBoolean, args("maxIter").toInt, args("epsilon").toDouble))
      case "SpatialStats.STSampling.randomSampling" =>
        featureRddList += (UUID -> Sampling.randomSampling(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("n").toInt))
      case "SpatialStats.STSampling.regularSampling" =>
        featureRddList += (UUID -> Sampling.regularSampling(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("x").toInt, args("y").toInt))
      case "SpatialStats.STSampling.stratifiedSampling" =>
        featureRddList += (UUID -> Sampling.stratifiedSampling(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("properties"), args("n").toInt))
      case "SpatialStats.STSampling.SandwichSampling" =>
        featureRddList += (UUID -> SandwichSampling.sampling(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("knowledge_title"), args("reporting_title"), args("accuracy").toDouble))
      case "SpatialStats.SpatialInterpolation.OrdinaryKriging" =>
        coverageRddList += (UUID -> Kriging.OrdinaryKriging(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt, args("method"), args("binMaxCount").toInt))
      case "SpatialStats.SpatialInterpolation.selfDefinedKriging" =>
        coverageRddList += (UUID -> Kriging.selfDefinedKriging(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt, args("method"), args("range").toDouble, args("sill").toDouble, args("nugget").toDouble))
      case "SpatialStats.SpatialInterpolation.NearestNeighbourInterpolation.fit" =>
        coverageRddList += (UUID -> NearestNeighbourInterpolation.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt))
      case "SpatialStats.SpatialInterpolation.LinearInterpolation.fit" =>
        coverageRddList += (UUID -> LinearInterpolation.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt))
      case "SpatialStats.SpatialInterpolation.IDW.fit" =>
        coverageRddList += (UUID -> IDW.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt, args("radiusX").toDouble, args("radiusY").toDouble, args("rotation").toDouble, args("weightingPower").toDouble, args("smoothingFactor").toDouble, args("equalWeightRadius").toDouble))
      case "SpatialStats.SpatialInterpolation.SplineInterpolation.thinPlateSpline" =>
        coverageRddList += (UUID -> SplineInterpolation.thinPlateSpline(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt))
      case "SpatialStats.SpatialInterpolation.SplineInterpolation.BSpline" =>
        coverageRddList += (UUID -> SplineInterpolation.BSpline(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyName"), args("rows").toInt, args("cols").toInt))

      case "SpatialStats.GWModels.GWRbasic.autoFit" =>
        val re_gwr = GWModels.GWRbasic.autoFit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("kernel"), args("approach"), args("adaptive").toBoolean)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GWRbasic.fit" =>
        val re_gwr = GWModels.GWRbasic.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GWRbasic.auto" =>
        val re_gwr = GWModels.GWRbasic.auto(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("kernel"), args("approach"), args("adaptive").toBoolean, args("varSelTh").toDouble)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GWRbasic.predict" =>
        val re_gwr = GWModels.GWRbasic.predict(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], featureRddList(args("predictRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean, args("approach"))
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GWRGeneralized.fit" =>
        val re_gwr = GWModels.GWRGeneralized.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("family"), args("kernel"), args("approach"), args("adaptive").toBoolean, args("tolerance").toDouble, args("maxIter").toInt)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GTWR.autoFit" =>
        val re_gwr = GWModels.GTWR.autoFit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("propertyT"), args("kernel"), args("approach"), args("adaptive").toBoolean, args("lambda").toDouble)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GTWR.fit" =>
        val re_gwr = GWModels.GTWR.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("propertyT"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean, args("lambda").toDouble)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GTWR.predict" =>
        val re_gwr = GWModels.GTWR.predict(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("propertiesT"), args("bandwidth").toDouble, args("kernel"), args("approach"), args("adaptive").toBoolean, args("lambda").toDouble)
        featureRddList += (UUID -> re_gwr)
      case "SpatialStats.GWModels.GWDA.calculate" =>
        val re_gwr = GWModels.GWDA.calculate(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean, args("method"))
        featureRddList += (UUID -> re_gwr)

      case "SpatialStats.GWModels.GWAverage" =>
        featureRddList += (UUID -> GWModels.GWAverage.cal(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean, args("quantile").toBoolean))
      case "SpatialStats.GWModels.GWCorrelation" =>
        featureRddList += (UUID -> GWModels.GWCorrelation.cal(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean))
      case "SpatialStats.GWModels.GWPCA" =>
        featureRddList += (UUID -> GWModels.GWPCA.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("properties"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean, args("k").toInt))
      case "SpatialStats.GWModels.MGWR" =>
        featureRddList += (UUID -> GWModels.MGWR.regress(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("kernel"), args("approach"), args("adaptive").toBoolean, args("iteration").toInt, args("epsilon").toDouble))


      //geodetector
      case "SpatialStats.SpatialHeterogeneity.GeoRiskDetector" =>
        stringList += (UUID -> Geodetector.riskDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
      case "SpatialStats.SpatialHeterogeneity.GeoFactorDetector" =>
        stringList += (UUID -> Geodetector.factorDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
      case "SpatialStats.SpatialHeterogeneity.GeoInteractionDetector" =>
        stringList += (UUID -> Geodetector.interactionDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
      case "SpatialStats.SpatialHeterogeneity.GeoEcologicalDetector" =>
        stringList += (UUID -> Geodetector.ecologicalDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))


        case "Coverage.sampleRegions" =>
          featureRddList += (UUID -> Coverage.sampleRegions(coverage = coverageRddList(args("coverage")), feature = featureRddList(args("feature")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], properties = args("properties").slice(1, args("properties").length - 1).split(',').toList, scale = if(args("scale")!="None") Some(args("scale").toDouble) else None, projection = if(args("projection")!="None") Some(args("projection")) else None))

        case "Geometry.Point" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadPoint(coords = getListFromJSONString(args("coordinates")), crs = args("crs")))
        case "Geometry.MultiPoint" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadMultiPoint(coords = getListFromJSONString(args("coordinates")), crs = args("crs")))
        case "Geometry.LineString" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadLineString(coords = getListFromJSONString(args("coordinates")), crs = args("crs"), geodesic = args("geodesic").toBoolean))
        case "Geometry.MultiLineString" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadMultiLineString(coords = getListFromJSONString(args("coordinates")), crs = args("crs"), geodesic = args("geodesic").toBoolean))
        case "Geometry.LinearRing" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadLinearRing(coords = getListFromJSONString(args("coordinates")), crs = args("crs"), geodesic = args("geodesic").toBoolean))
        case "Geometry.Polygon" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadPolygon(coords = getListFromJSONString(args("coordinates")), crs = args("crs"), geodesic = args("geodesic").toBoolean))
        case "Geometry.MultiPolygon" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadMultiPolygon(coords = getListFromJSONString(args("coordinates")), crs = args("crs"), geodesic = args("geodesic").toBoolean))
        case "Geometry.GeometryCollection" =>
          ogeGeometryList += (UUID -> OGEGeometry.loadGeometryCollection(coords = getListFromJSONString(args("coordinates")), crs = args("crs"), geodesic = args("geodesic").toBoolean))
        case "Geometry.addStyles" =>
          OGEGeometry.visualize(sc, geom = ogeGeometryList(args("input")), color = args("color").replace("[", "").replace("]", "").split(',').toList, attribute = args("attribute"))
        case "Geometry.reproject" =>
          ogeGeometryList += (UUID -> OGEGeometry.reproject(geom = ogeGeometryList(args("geometry")), tarCrsCode = args("crs")))

        case "Feature.loadFromGeometry" =>
          featureList += (UUID -> Feature.loadFromGeometry(geometry = ogeGeometryList(args("geometry")), properties = Feature.getMapFromStr(args("metadata"))))
        case "Feature.addStyles" =>
          Feature.visualize(sc, feature = featureList(args("input")), color = args("color").replace("[", "").replace("]", "").split(',').toList, attribute = args("attribute"))
        case "Feature.loadFromGeojson" =>
          featureList += (UUID -> Feature.loadFromGeojson(gjson = args("gjson"), properties = if (!args.contains("properties")) mutable.Map.empty[String, Any] else Feature.getMapFromStr(args("properties")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.load" =>
          featureList += (UUID -> Feature.load(featureId = args("featureId"), productId = args("productId"), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.loadFeatureFromUpload" =>
          featureList += (UUID -> Feature.loadFeatureFromUpload(featureId = args("featureId"), userID = userId, dagID = dagId, crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.area" =>
          doubleList += (UUID -> Feature.area(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.bounds" =>
          featureList += (UUID -> Feature.bounds(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.buffer" =>
          featureList += (UUID -> Feature.buffer(featureList(args("feature")),
            distance = args("distance").toDouble,
            crs = if (!args.contains("crs")) null else args("crs")))
        case "Feature.centroid" =>
          featureList += (UUID -> Feature.centroid(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.closestPoint" =>
          featureList += (UUID -> Feature.closestPoint(featureList(args("feature1")),featureList(args("feature2")),
            terminateDistance = args("terminateDistance").toDouble))
        case "Feature.closestPoints" =>
          featureList += (UUID -> Feature.closestPoints(featureList(args("feature1")), featureList(args("feature2")),
            terminateDistance = args("terminateDistance").toDouble))
        case "Feature.convexHull" =>
          featureList += (UUID -> Feature.convexHull(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.containedIn" =>
          stringList += (UUID -> Feature.contains(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")).toString)
        case "Feature.contains" =>
          stringList += (UUID -> Feature.containedIn(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")).toString)
        case "Feature.copyProperties" =>
          featureList += (UUID -> Feature.copyProperties(featureList(args("feature1")),featureList(args("feature2")),
            if (!args.contains("properties")) null else args("properties").replace("[", "").replace("]", "").split(",").toList,
            if (!args.contains("exclude")) null else args("exclude").replace("[", "").replace("]", "").split(",").toList))
        case "Feature.cutLines" =>
          featureList += (UUID -> Feature.cutLines(featureList(args("feature")),
            args("distances").replace("[", "").replace("]", "").split(",").toList.map(_.trim.toDouble),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.difference" =>
          featureList += (UUID -> Feature.difference(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.symmetricDifference" =>
          featureList += (UUID -> Feature.symmetricDifference(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.disjoint" =>
          stringList += (UUID -> Feature.disjoint(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")).toString)
        case "Feature.dissolve" =>
          featureList += (UUID -> Feature.dissolve(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.distance" =>
          stringList += (UUID -> Feature.distance(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")).toString)
        case "Feature.withinDistance" =>
          stringList += (UUID -> Feature.withinDistance(featureList(args("feature1")), featureList(args("feature2")),
            distance = args("distance").toDouble,
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")).toString)
        case "Feature.intersection" =>
          featureList += (UUID -> Feature.intersection(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.union" =>
          featureList += (UUID -> Feature.union(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.intersects" =>
          stringList += (UUID -> Feature.intersects(featureList(args("feature1")), featureList(args("feature2")),
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")).toString)
        case "Feature.length" =>
          doubleList += (UUID -> Feature.length(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.perimeter" =>
          doubleList += (UUID -> Feature.perimeter(featureList(args("feature")), crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "Feature.perimeter" =>
          stringList += (UUID -> Feature.propertyNames(featureList(args("feature"))).toString())
        case "Feature.get" =>
          stringList += (UUID -> Feature.get(featureList(args("feature")), args("property")).toString)
        case "Feature.getNumber" =>
          stringList += (UUID -> Feature.getNumber(featureList(args("feature")), args("property")).toString)
        case "Feature.getString" =>
          stringList += (UUID -> Feature.getString(featureList(args("feature")), args("property")))
        case "Feature.id" =>
          stringList += (UUID -> Feature.id(featureList(args("feature"))))
        case "Feature.geometry" =>
          stringList += (UUID -> Feature.geometry(featureList(args("feature")), if (!args.contains("geodesics")) None else Some(true)))
        case "Feature.toArray" =>
          stringList += (UUID -> Feature.toArray(featureList(args("feature")),
            args("properties").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim)).mkString)
        case "Feature.toDictionary" =>
          stringList += (UUID -> Feature.toDictionary(featureList(args("feature")),
            if (!args.contains("properties")) null else args("properties").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim)).toString())
        case "Feature.reproject" =>
          featureList += (UUID -> Feature.reproject(featureList(args("feature")), args("tarCrsCode")))
        case "Feature.set" =>
          featureList += (UUID -> Feature.set(featureList(args("feature")), args("var_args")))
        case "Feature.setGeometry" =>
          featureList += (UUID -> Feature.setGeometry(featureList(args("feature")), geometry = ogeGeometryList(args("geometry"))))
        case "Feature.select" =>
          featureList += (UUID -> Feature.select(featureList(args("feature")),
            args("propertySelectors").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            if (!args.contains("newProperties")) null else args("newProperties").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            retainGeometry = if (!args.contains("retainGeometry")) true else args("retainGeometry").toBoolean))
        case "Feature.simplify" =>
          featureList += (UUID -> Feature.simplify(featureList(args("feature")),
            distanceTolerance = args("distanceTolerance").toDouble,
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs"),
            topoPreserved = if (!args.contains("topoPreserved")) false else args("topoPreserved").toBoolean))
        case "Feature.densify" =>
          featureList += (UUID -> Feature.densify(featureList(args("feature")),
            distanceTolerance = args("distanceTolerance").toDouble,
            crs = if (!args.contains("crs")) "EPSG:4326" else args("crs")))
        case "FeatureCollection.loadFromGeometry" => // oge.FeatureCollection(geometry)
          featureCollectionList += (UUID -> FeatureCollection.loadFromGeometry(sc, ogeGeometryList(args("geometry")), properties = Feature.getMapFromStr(args("metadata")), id = args("id")))
        case "FeatureCollection.loadFromFeature" => // oge.FeatureCollection(feature)
          featureCollectionList += (UUID -> FeatureCollection.loadFromFeature(sc, featureList(args("feature"))))
        case "FeatureCollection.loadFromFeatureList" => // oge.FeatureCollection([feature1, feature2])
          featureCollectionList += (UUID -> FeatureCollection.loadFromFeatureList(sc, args("features").replace("[", "").replace("]", "").split(',').toList.map(f=>featureList(f.trim))))
        case "FeatureCollection.load" => //和Service.getFeatureCollection是一致的，FeatureCollection.load还未在algorithms.json中注册，这里先保留这种形式
          featureCollectionList += (UUID -> FeatureCollection.load(sc, productId = args("productId"), extent = if (!args.contains("extent")) null else ogeGeometryList(args("extent")),
            startTime = isOptionalArg(args, "startTime"), endTime = isOptionalArg(args, "endTime"),
            crs = if (!args.contains("crs")) null else args("crs")))
        case "FeatureCollection.loadFeatureCollectionFromUpload" =>
          featureCollectionList += (UUID -> FeatureCollection.loadFeatureCollectionFromUpload(sc, featureId = args("featureId"), userID = userId, dagID = dagId, crs = args("crs")))
        case "FeatureCollection.loadFromGeojson" =>
          featureCollectionList += (UUID -> FeatureCollection.loadFromGeojson(sc, args("gjson"), args("crs")))
        case "FeatureCollection.addStyles" =>
          FeatureCollection.visualize(featureCollection = featureCollectionList(args("input")), color = args("color").replace("[", "").replace("]", "").split(',').toList, attribute = args("attribute"))
        case "FeatureCollection.export" =>
          FeatureCollection.exportAndUploadFeatures(sc, featureCollectionList(args("featureCollection")), batchParam, dagId)
        case "FeatureCollection.loadAllTableElements" =>
          featureCollectionList += (UUID -> FeatureCollection.loadAllTableElements(sc, table = args("tableName"),
            geometryColumn = if (args.contains("geometryColumn")) args("geometryColumn") else null))
        case "FeatureCollection.loadWithRestrictions" =>
          featureCollectionList += (UUID -> FeatureCollection.loadWithRestrictions(sc, query = args("queryInSQL"),
            geometryColumn = if (args.contains("geometryColumn")) args("geometryColumn") else null))
        case "FeatureCollection.spatialPredicateQuery" =>
          featureCollectionList += (UUID -> FeatureCollection.spatialPredicateQuery(featureCollectionList(args("collection")), ogeGeometryList(args("geometry")), args("geometryFieldName"),args("spatialPredicate")))
        case "FeatureCollection.differenceWithGeometry" =>
          featureCollectionList += (UUID -> FeatureCollection.differenceWithGeometry(sc, featureCollectionList(args("collection")), ogeGeometryList(args("geometry")), args("geometryFieldName")))
        case "FeatureCollection.aggregateArray" =>
          stringList += (UUID -> FeatureCollection.aggregate_array(featureCollectionList(args("collection")),property = args("property")).toString())
        case "FeatureCollection.aggregateCount" =>
          intList += (UUID -> FeatureCollection.aggregate_count(featureCollectionList(args("collection")), property = args("property")))
        case "FeatureCollection.aggregateCountDistinct" =>
          intList += (UUID -> FeatureCollection.aggregate_count_distinct(featureCollectionList(args("collection")), property = args("property")))
        case "FeatureCollection.aggregateFirst" =>
          stringList += (UUID -> FeatureCollection.aggregate_first(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateMax" =>
          stringList += (UUID -> FeatureCollection.aggregate_max(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateMin" =>
          stringList += (UUID -> FeatureCollection.aggregate_min(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateMean" =>
          stringList += (UUID -> FeatureCollection.aggregate_mean(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateProduct" =>
          stringList += (UUID -> FeatureCollection.aggregate_product(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateSampleSD" =>
          stringList += (UUID -> FeatureCollection.aggregate_sample_sd(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateSampleVAR" =>
          stringList += (UUID -> FeatureCollection.aggregate_sample_var(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateTotalSD" =>
          stringList += (UUID -> FeatureCollection.aggregate_total_sd(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateTotalVAR" =>
          stringList += (UUID -> FeatureCollection.aggregate_total_var(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateStats" =>
          stringList += (UUID -> FeatureCollection.aggregate_stats(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.aggregateTotalSum" =>
          stringList += (UUID -> FeatureCollection.aggregate_total_sum(featureCollectionList(args("collection")), property = args("property")).toString)
        case "FeatureCollection.distinct" =>
          featureCollectionList += (UUID -> FeatureCollection.distinct(featureCollectionList(args("collection")),
            if (!args.contains("property")) null else
            args("property").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim)))
        case "FeatureCollection.describe" =>
          featureCollectionList += (UUID -> FeatureCollection.describe(featureCollectionList(args("collection")),
            args("property").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim)))
        case "FeatureCollection.limit" =>
          featureCollectionList += (UUID -> FeatureCollection.limit(featureCollectionList(args("collection")), args("maxRows").toInt,
            if(args.contains("property")) args("property") else null, if (args.contains("ascending")) args("ascending").toBoolean else true))
        case "FeatureCollection.assembleNumericArray" =>
          featureCollectionList += (UUID -> FeatureCollection.assembleNumericArray(featureCollectionList(args("collection")),
            args("properties").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            if (args.contains("newPropertyName")) args("newPropertyName") else "array"))
        case "FeatureCollection.propertyNames" =>
          stringList += (UUID -> FeatureCollection.propertyNames(featureCollectionList(args("collection"))).toString())
        case "FeatureCollection.propertySet" =>
          featureCollectionList += (UUID -> FeatureCollection.propertySet(featureCollectionList(args("collection")), args("columnName"), args("newContent")))
        case "FeatureCollection.randomColumn" =>
          featureCollectionList += (UUID -> FeatureCollection.randomColumn(featureCollectionList(args("collection")),
            if (args.contains("columnName")) args("columnName") else "random",
            if (args.contains("seed")) args("newPropertyName").toLong else 0,
            if (args.contains("distribution")) args("distribution") else "uniform",
            args("rowKeys").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim)))
        case "FeatureCollection.intersection" =>
          featureCollectionList += (UUID -> FeatureCollection.intersection(sc, featureCollectionList(args("collection1")), featureCollectionList(args("collection2")),
            if (!args.contains("geometryFieldName1")) "geom" else args("geometryFieldName1"), if (!args.contains("geometryFieldName2")) "geom" else args("geometryFieldName2")))
        case "FeatureCollection.erase" =>
          featureCollectionList += (UUID -> FeatureCollection.erase(sc, featureCollectionList(args("collection1")), featureCollectionList(args("collection2")),
            if (!args.contains("geometryFieldName1")) "geom" else args("geometryFieldName1"), if (!args.contains("geometryFieldName2")) "geom" else args("geometryFieldName2")))
        case "FeatureCollection.difference" =>
          featureCollectionList += (UUID -> FeatureCollection.difference(sc, featureCollectionList(args("collection1")), featureCollectionList(args("collection2")),
            if (!args.contains("geometryFieldName1")) "geom" else args("geometryFieldName1"), if (!args.contains("geometryFieldName2")) "geom" else args("geometryFieldName2")))
        case "FeatureCollection.union" =>
          featureCollectionList += (UUID -> FeatureCollection.union(sc, featureCollectionList(args("collection1")), featureCollectionList(args("collection2")),
            if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.map" =>
          featureCollectionList += (UUID -> FeatureCollection.map(featureCollectionList(args("collection")), args("baseAlgorithm"), if (args.contains("dropNulls")) args("dropNulls").toBoolean else true))
        case "FeatureCollection.remap" =>
          featureCollectionList += (UUID -> FeatureCollection.remap(featureCollectionList(args("collection")),
            args("lookupIn").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            args("lookupOut").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            args("property")))
        case "FeatureCollection.select" =>
          featureCollectionList += (UUID -> FeatureCollection.select(featureCollectionList(args("collection")),
            args("propertySelectors").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            if (!args.contains("newProperties")) null else args("newProperties").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            if (args.contains("retainGeometry")) args("retainGeometry").toBoolean else true))
        case "FeatureCollection.extractRowsAsList" =>
          featureCollectionList += (UUID -> FeatureCollection.extractRowsAsList(featureCollectionList(args("collection")),
            args("count").toInt,
            if (args.contains("offset")) args("offset").toInt else 0))
        case "FeatureCollection.spatialJoinOneToOne" =>
          featureCollectionList += (UUID -> FeatureCollection.spatialJoinOneToOne(sc, featureCollectionList(args("leftCollection")), featureCollectionList(args("rightCollection")),
            args("reduceProperties").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            args("reduceRules").replace("[", "").replace("]", "").split(',').toList.map(s => s.trim),
            args("leftGeometryFieldName"), args("rightGeometryFieldName"), args("useIndex").toBoolean, args("spatialPredicate")))
        case "FeatureCollection.spatialJoinQueryFlat" =>
          featureCollectionList += (UUID -> FeatureCollection.spatialJoinQueryFlat(sc, featureCollectionList(args("leftSpatialDataFrame")), featureCollectionList(args("rightCollection")),
            if (!args.contains("leftGeometryFieldName")) "geom" else args("leftGeometryFieldName"), if (!args.contains("rightGeometryFieldName")) "geom" else args("rightGeometryFieldName"),
            if(!args.contains("spatialPredicate")) "Contains" else args("spatialPredicate"),
            if (!args.contains("useRDD")) false else args("useRDD").toBoolean,
            if (!args.contains("useIndex")) true else args("useIndex").toBoolean,
            if (!args.contains("gridType")) "QUADTREE" else args("gridType")))
        case "FeatureCollection.spatialJoinQuery" =>
          featureCollectionList += (UUID -> FeatureCollection.spatialJoinQuery(sc, featureCollectionList(args("leftSpatialDataFrame")), featureCollectionList(args("rightCollection")),
            if (!args.contains("leftGeometryFieldName")) "geom" else args("leftGeometryFieldName"), if (!args.contains("rightGeometryFieldName")) "geom" else args("rightGeometryFieldName"),
            if (!args.contains("spatialPredicate")) "Contains" else args("spatialPredicate"),
            if (!args.contains("useRDD")) false else args("useRDD").toBoolean,
            if (!args.contains("useIndex")) true else args("useIndex").toBoolean,
            if (!args.contains("gridType")) "QUADTREE" else args("gridType")))
        case "FeatureCollection.distanceJoinQueryFlat" =>
          featureCollectionList += (UUID -> FeatureCollection.distanceJoinQueryFlat(sc, featureCollectionList(args("spatialDataFrame")), featureCollectionList(args("queryDataFrame")),
            if (!args.contains("spatialGeometryFieldName")) "geom" else args("spatialGeometryFieldName"), if (!args.contains("queryGeometryFieldName")) "geom" else args("queryGeometryFieldName"),
            args("distance").toDouble,
            if (!args.contains("limit")) "less_than" else args("limit"),
            if (!args.contains("useRDD")) false else args("useRDD").toBoolean,
            if (!args.contains("useIndex")) true else args("useIndex").toBoolean,
            if (!args.contains("spatialPredicate")) "Contains" else args("spatialPredicate"),
            if (!args.contains("gridType")) "QUADTREE" else args("gridType")))
        case "FeatureCollection.distanceJoinQuery" =>
          featureCollectionList += (UUID -> FeatureCollection.distanceJoinQuery(sc, featureCollectionList(args("spatialDataFrame")), featureCollectionList(args("queryDataFrame")),
            if (!args.contains("spatialGeometryFieldName")) "geom" else args("spatialGeometryFieldName"), if (!args.contains("queryGeometryFieldName")) "geom" else args("queryGeometryFieldName"),
            args("distance").toDouble,
            if (!args.contains("limit")) "less_than" else args("limit"),
            if (!args.contains("useRDD")) false else args("useRDD").toBoolean,
            if (!args.contains("useIndex")) true else args("useIndex").toBoolean,
            if (!args.contains("spatialPredicate")) "Contains" else args("spatialPredicate"),
            if (!args.contains("gridType")) "QUADTREE" else args("gridType")))
        case "FeatureCollection.kNNQuery" =>
          featureCollectionList += (UUID -> FeatureCollection.kNNQuery(sc, featureCollectionList(args("spatialDataFrame")), ogeGeometryList(args("queryGeometry")),
            args("k").toInt,
            if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName"),
            if (!args.contains("useRDD")) false else args("useRDD").toBoolean,
            if (!args.contains("useIndex")) true else args("useIndex").toBoolean,
            if (!args.contains("spatialPredicate")) "Contains" else args("spatialPredicate"),
            if (!args.contains("gridType")) "QUADTREE" else args("gridType")))
        case "FeatureCollection.idwInterpolation" =>
          coverageRddList += (UUID -> FeatureCollection.interpolateByIDW(sc,
            featureCollectionList(args("collection")),
            args("propertyName"),
            ogeGeometryList(args("maskGeom")),
            if (args.contains("cols")) args("cols").toInt else 256,
            if (args.contains("rows")) args("rows").toInt else 256,
            if (args.contains("defaultValue")) args("defaultValue").toDouble else 100.0))
        case "FeatureCollection.simpleKriging" =>
          coverageRddList += (UUID -> FeatureCollection.simpleKriging(sc,
            featureCollectionList(args("collection")),
            args("propertyName"),
            if (args.contains("cols")) args("cols").toInt else 256,
            if (args.contains("rows")) args("rows").toInt else 256,
            if (args.contains("defaultValue")) args("defaultValue").toDouble else 100.0))
        case "FeatureCollection.size" =>
          stringList += (UUID -> FeatureCollection.size(featureCollectionList(args("collection"))).toString)
        case "FeatureCollection.area" =>
          featureCollectionList += (UUID -> FeatureCollection.area(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.length" =>
          featureCollectionList += (UUID -> FeatureCollection.length(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.boundary" =>
          featureCollectionList += (UUID -> FeatureCollection.boundary(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.perimeter" =>
          featureCollectionList += (UUID -> FeatureCollection.perimeter(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.geoHash" =>
          featureCollectionList += (UUID -> FeatureCollection.geoHash(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName"),
            if (args.contains("geoHashPrecision")) args("geoHashPrecision").toInt else 10))
        case "FeatureCollection.buffer" =>
          featureCollectionList += (UUID -> FeatureCollection.buffer(sc, featureCollectionList(args("collection")), args("distance").toDouble, if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.delaunay" =>
          featureCollectionList += (UUID -> FeatureCollection.delaunay(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.voronoi" =>
          featureCollectionList += (UUID -> FeatureCollection.voronoi(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.subtract" =>
          featureCollectionList += (UUID -> FeatureCollection.subtract(featureCollectionList(args("collection")), args("column1"), args("column2"), args("newColumnName")))
        case "FeatureCollection.combine" =>
          featureCollectionList += (UUID -> FeatureCollection.combine(featureCollectionList(args("collection")), args("column1"), args("column2"), args("newColumnName")))
        case "FeatureCollection.constantColumn" =>
          featureCollectionList += (UUID -> FeatureCollection.constantColumn(featureCollectionList(args("collection")), args("constantValue").asInstanceOf[Any], if (!args.contains("columnName")) "constant" else args("columnName")))
        case "FeatureCollection.reproject" =>
          featureCollectionList += (UUID -> FeatureCollection.reproject(sc, featureCollectionList(args("collection")),
            if (!args.contains("tarCrsCode")) "EPSG:4326" else args("tarCrsCode"),
            if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.sample" =>
          featureCollectionList += (UUID -> FeatureCollection.sample(
            featureCollectionList(args("collection")), args("fraction").toDouble,
            if (args.contains("seed")) args("seed").toLong else 0,
            if (args.contains("allowDuplicate")) args("allowDuplicate").toBoolean else false))
        case "FeatureCollection.sortAndSplit" =>
          featureCollectionList += (UUID -> FeatureCollection.sortAndSplit(
            featureCollectionList(args("collection")), args("property"),
            if (args.contains("ascending")) args("seed").toBoolean else true,
            if (args.contains("geoHashPrecision")) args("geoHashPrecision").toInt else 10))
        case "FeatureCollection.randomSplit" =>
          val splits = FeatureCollection.randomSplit(
            featureCollectionList(args("collection")),
            args("weights").replace("[", "").replace("]", "").split(",").map(_.trim.toDouble),
            if (args.contains("seed")) args("seed").toLong else 0
          )
          splits.zipWithIndex.foreach { case (df, idx) =>
            val newKey = s"${args("collection")}_split$idx"
            featureCollectionList += (newKey -> df)
          }
        case "FeatureCollection.filterBounds" =>
          featureCollectionList += (UUID -> FeatureCollection.filterBounds(sc, featureCollectionList(args("collection")), ogeGeometryList(args("geometry")),
            if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName"),
            if (!args.contains("useRDD")) false else args("useRDD").toBoolean))
        case "FeatureCollection.filterDate" =>
          featureCollectionList += (UUID -> FeatureCollection.filterDate(featureCollectionList(args("collection")), args("start"), args("end"), if (!args.contains("timeFieldName")) "time" else args("timeFieldName")))
        case "FeatureCollection.filterMetadata" =>
          featureCollectionList += (UUID -> FeatureCollection.filterMetadata(featureCollectionList(args("collection")), args("name"), args("operator"), args("value")))
        case "FeatureCollection.aggregateUnion" =>
          featureCollectionList += (UUID -> FeatureCollection.aggregate_union(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.aggregateIntersection" =>
          featureCollectionList += (UUID -> FeatureCollection.aggregate_intersection(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.merge" =>
          featureCollectionList += (UUID -> FeatureCollection.merge(featureCollectionList(args("collection1")), featureCollectionList(args("collection2"))))
        case "FeatureCollection.mergeAll" =>
          featureCollectionList += (UUID -> FeatureCollection.mergeAll(args("collectionList").replace("[", "").replace("]", "").split(',').toList.map(f=>featureCollectionList(f.trim))))
        case "FeatureCollection.geometryGetAndMerge" =>
          ogeGeometryList += (UUID -> FeatureCollection.geometryGetAndMerge(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.aggregateBounds" =>
          ogeGeometryList += (UUID -> FeatureCollection.aggregate_bounds(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "FeatureCollection.rasterize" =>
          coverageRddList += (UUID -> FeatureCollection.rasterize(featureCollectionList(args("collection")), if (!args.contains("geometryFieldName")) "geom" else args("geometryFieldName")))
        case "Feature.export" =>
          (UUID-> Feature.exportFeature(sc,
            featureRddList(args("feature")).asInstanceOf[RDD[(String, (Geometry,
              mutable.Map[String, Any]))]],args.getOrElse("fileType","geojson"),
            batchParam,dagId
          ))
        case "FeatureCollection.cultivatedLandSuitability" =>
          featureCollectionList += (UUID -> FeatureCollection.cultivatedLandSuitability(sc, args("landTable"), args("ecologyTable"), args("urbanTable"), args("slopeTable")))
        case _ =>
          logger.info(s"将从插件调用算子：$funcName")
          if (!PluginLoader.executors.contains(funcName)) {
            val message = s"不存在名为${funcName}的插件！"
            logger.error(message)
            throw new UnsupportedOperationException(message)
          }
          val executor = PluginLoader.executors(funcName)
          val argValues = resolveDAGParams(args)
          val result = executor.execute(sc, argValues)
          dagResultCacheMap += (UUID -> result)
          result match {
            case _: IntData =>
              intList += (UUID -> result.asInstanceOf[IntData].data)
            case _: LongData =>
              intList += (UUID -> result.asInstanceOf[LongData].data.toInt)
            case _: DoubleData =>
              doubleList += (UUID -> result.asInstanceOf[DoubleData].data)
            case _: StringData =>
              stringList += (UUID -> result.asInstanceOf[StringData].data)
            case _: CoverageRDD =>
              val coverage = result.asInstanceOf[CoverageRDD]
              coverageRddList += (UUID -> (coverage.data, coverage.metadata))
            case _: CoverageCollectionRDD =>
              val coverageCollection = result.asInstanceOf[CoverageCollectionRDD]
              val collectionMap = mutable.Map.empty[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]
              for ((key, value) <- coverageCollection.data) {
                collectionMap += (key -> (value.data, value.metadata))
              }
              coverageCollectionRddList += (UUID -> collectionMap.toMap)
            case _: FeatureRDD =>
              featureRddList += (UUID -> result.asInstanceOf[FeatureRDD].data)
            case _ =>
              val message = s"Unsupported Type: ${result.getClass.getName}"
              logger.error(message)
              throw new IllegalArgumentException(message)
          }
      }
    } catch {
      case e: InvocationTargetException =>
        throw e.getCause()
      case e: Throwable =>
        Trigger.optimizedDagMap.clear()
        Trigger.coverageCollectionMetadata.clear()
        Trigger.lazyFunc.clear()
        Trigger.coverageCollectionRddList.clear()
        Trigger.coverageRddList.clear()
        Trigger.zIndexStrArray.clear()
        JsonToArg.dagMap.clear()
        Trigger.tableRddList.clear()
        Trigger.kernelRddList.clear()
        Trigger.mlmodelRddList.clear()
        Trigger.featureRddList.clear()
        Trigger.cubeRDDList.clear()
        Trigger.cubeLoad.clear()

        throw e
    }
  }


  /**
   *Determine whether a node with a certain ID in the entire DAG exists
   *
   * @param dagNodeId DAG node id
   * @return does it exist
   */
  private def dagNodeExists(dagNodeId: String): Boolean = {
    if (!optimizedDagMap.contains("0")) {
      val message = s"DAG list is not exists! Cannot parse DAG args. Map: $optimizedDagMap"
      logger.error(message)
      throw new RuntimeException(message)
    }
    val dagList = optimizedDagMap("0")
    dagList.count(node => node._1 == dagNodeId) > 0
  }

  /**
   *Parse the parameter list of the DAG node into actual values<br>
   *Each DAG node parameter list is of type Map[String, String], the key is the DAG node id, and the value is the parameter value, or a reference to the upstream DAG, therefore:
   *<ul>
   *<li>If the value is not a node id that already exists in the DAG list, it means it is a constant parameter value and can be extracted directly</li>
   *<li>If the value is a node id in the DAG list, it means that it is a reference to the execution result of the DAG node, and the actual value is extracted from the cache list</li>
   *</ul>
   *Finally, we get Map[String, AllowedData], which is a list of actual values ​​corresponding to the parameter names input by the adaptation plug-in operator.
   *
   * @param dagArgs Parameter list for each DAG node
   * @return parsed parameter list containing actual values
   */
  private def resolveDAGParams(dagArgs: mutable.Map[String, String]): mutable.Map[String, AllowedData] = {
    val result = mutable.Map.empty[String, AllowedData]
    for ((name, value) <- dagArgs) {
      if (dagNodeExists(value)) {
        if (dagResultCacheMap.contains(value)) {
          result.put(name, dagResultCacheMap(value))
        } else if (intList.contains(value)) { 
          result.put(name, IntData(intList(value)))
        } else if (doubleList.contains(value)) {
          result.put(name, DoubleData(doubleList(value)))
        } else if (stringList.contains(value)) {
          result.put(name, StringData(stringList(value)))
        } else if (coverageRddList.contains(value)) {
          val coverage = coverageRddList(value)
          result.put(name, CoverageRDD(coverage._2, coverage._1))
        } else if (coverageCollectionRddList.contains(value)) {
          val coverageMap = mutable.Map.empty[String, CoverageRDD]
          for ((name, coverage) <- coverageCollectionRddList(value)) {
            coverageMap.put(name, CoverageRDD(coverage._2, coverage._1))
          }
          result.put(name, CoverageCollectionRDD(coverageMap))
        } else if (featureRddList.contains(value)) {
          val feature = featureRddList(value).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]
          result.put(name, FeatureRDD(feature))
        }
      } else {
        result.put(name, StringData(value))
      }
    }
    result
  }

  /**
   *Execute all nodes of the entire DAG
   *
   * @param sc Spark context, implicitly passed automatically
   * @param list DAG node list
   */
  def lambda(implicit sc: SparkContext, list: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): Unit = {
    for (i <- list.indices) {
      try {
        func(sc, list(i)._1, list(i)._2, list(i)._3)
      } catch {
        case e: Throwable =>
          throw new Exception(e)
      }
    }
  }

  def optimizedDAG(list: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    val duplicates: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = removeDuplicates(list)
    val checked: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = checkMap(duplicates)
    checked
  }

  def checkMap(collection: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    var result: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = collection
    for ((_, second, third) <- result) {
      if (second == "Collection.map") {
        for (tuple <- third) {
          if (tuple._1 == "baseAlgorithm") {
            for ((f, s, t) <- collection) {
              if (f == tuple._2) {
                lazyFunc += (f -> (s, t))
                result = result.filter(t => t._1 != f)
              }
            }
          }
        }
      }
    }
    result
  }

  def removeDuplicates(collection: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    val seen: mutable.HashMap[(String, mutable.Map[String, String]), String] = mutable.HashMap[(String, mutable.Map[String, String]), String]()
    val result: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]()
    val duplicates: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer[(String, String)]()

    for ((first, second, third) <- collection) {
      val key: (String, mutable.Map[String, String]) = (second, third)
      if (seen.contains(key)) {
        duplicates += Tuple2(first, seen(key))
      } else {
        seen(key) = first
        result += ((first, second, third))
      }
    }

    for ((_, _, third) <- result) {
      for (duplicate <- duplicates) {
        for (tuple <- third) {
          if (tuple._2 == duplicate._1) {
            third.remove(tuple._1)
            third.put(tuple._1, duplicate._2)
          }
        }
      }
    }
    result
  }

  /**
   *Development Center-Script Execution Entrance
   *
   * @param sc Spark context object, implicitly passed automatically
   * @param curWorkTaskJson The incoming DAG JSON content is passed by DAG Boot after parsing by the Scripts component.
   * @param curDagID current DAG id
   * @param userID current user id
   */
  def runMain(implicit sc: SparkContext, curWorkTaskJson: String, curDagID: String, userID: String): Unit = {
    if (sc.isStopped) {
      sendPost(DAG_ROOT_URL + "/deliverUrl", "ERROR")
      println("Send to boot!,when sc is stopped")
      return
    }
    workType = "main"
    workTaskJson = curWorkTaskJson
    val dagJsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(dagJsonObject)
    dagId = curDagID
    userId = userID
    val time1: Long = System.currentTimeMillis()
    try {
      if (dagJsonObject.containsKey("dagType") && dagJsonObject.getString("dagType").equals("edu")) {
        dagType = "edu"
        outputFile = dagJsonObject.getString("outputFile")
      }
    } catch {
      case e: Exception =>
        dagType = ""
        outputFile = ""
        logger.error(s"Exception occurred when get property 'dagType'", e)
    }
    isBatch = dagJsonObject.getString("isBatch").toInt
    layerName = dagJsonObject.getString("layerName")
    try {
      val map: JSONObject = dagJsonObject.getJSONObject("map")
      level = map.getString("level").toInt
      val spatialRange: Array[Double] = map.getString("spatialRange")
        .substring(1, map.getString("spatialRange").length - 1).split(",").map(_.toDouble)
      println("spatialRange = " + spatialRange.mkString("Array(", ", ", ")"))
      windowExtent = Extent(spatialRange.head, spatialRange(1), spatialRange(2), spatialRange(3))
      println("WindowExtent", windowExtent.xmin, windowExtent.ymin, windowExtent.xmax, windowExtent.ymax)
    } catch {
      case e: Exception =>
        level = 11
        windowExtent = null
        logger.error(s"Exception occurred when parsing map properties, using the default values, level=$level, windowExtent=$windowExtent", e)
    }
    println("***********************************************************")
    if (sc.master.contains("local")) {
      JsonToArg.jsonAlgorithms = GlobalConfig.Others.jsonAlgorithms
    }
    JsonToArg.trans(dagJsonObject, "0")
    println("JsonToArg.dagMap.size = " + JsonToArg.dagMap)
    JsonToArg.dagMap.foreach(DAGList => {
      println("************优化前的DAG*************")
      println(DAGList._1)
      DAGList._2.foreach(println(_))
      println("************优化后的DAG*************")
      val optimizedDAGList: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = optimizedDAG(DAGList._2)
      optimizedDagMap += (DAGList._1 -> optimizedDAGList)
      optimizedDAGList.foreach(println(_))
      println("***********************************")
    })
    try {
      lambda(sc, optimizedDagMap("0"))
      PostSender.sendShelvedPost()
    } catch {
      case e: Throwable =>
        val errorJson = new JSONObject
        errorJson.put("error", e.getCause.getMessage)
        val outJsonObject: JSONObject = new JSONObject
        outJsonObject.put("workID", Trigger.dagId)
        outJsonObject.put("json", errorJson)
        //
        println("Error json = " + outJsonObject)
        sendPost(DAG_ROOT_URL + "/deliverUrl",
          outJsonObject.toJSONString)
        println("Send to boot!")
        logger.error(s"Executing failed! Error JSON: $outJsonObject", e)
    } finally {
      Trigger.outputInformationList.clear()
      Trigger.optimizedDagMap.clear()
      Trigger.coverageCollectionMetadata.clear()
      Trigger.lazyFunc.clear()
      Trigger.coverageCollectionRddList.clear()
      Trigger.coverageRddList.clear()
      Trigger.zIndexStrArray.clear()
      JsonToArg.dagMap.clear()
      Trigger.tableRddList.clear()
      Trigger.kernelRddList.clear()
      Trigger.mlmodelRddList.clear()
      Trigger.featureRddList.clear()
      Trigger.cubeRDDList.clear()
      Trigger.cubeLoad.clear()
      Trigger.intList.clear()
      Trigger.doubleList.clear()
      Trigger.stringList.clear()
      PostSender.clearShelvedMessages()
      tempFileList.foreach(tempFile => {
        if (scala.reflect.io.File(tempFile).exists)
          scala.reflect.io.File(tempFile).delete()
      })
      val time2: Long = System.currentTimeMillis()
      println(time2 - time1)
    }
  }

  /**
   *Development Center-Batch task execution entrance
   *
   * @param sc Spark context object, implicitly passed automatically
   * @param curWorkTaskJson The incoming DAG JSON content is passed by DAG Boot after parsing by the Scripts component.
   * @param curDagId current DAG id
   * @param userID current user id
   * @param crs output layer coordinate system
   * @param scale output layer scaling
   * @param folder output folder
   * @param fileName output file name
   * @param format output format
   */
  def runBatch(implicit sc: SparkContext, curWorkTaskJson: String, curDagId: String, userID: String, crs: String, scale: String, folder: String, fileName: String, format: String): Unit = {
    workTaskJson = curWorkTaskJson
    val dagJsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(dagJsonObject)
    dagId = curDagId
    userId = userID
    val time1: Long = System.currentTimeMillis()
    isBatch = dagJsonObject.getString("isBatch").toInt
    windowExtent = null
    batchParam.setUserId(userID)
    batchParam.setDagId(curDagId)
    batchParam.setCrs(crs)
    batchParam.setFolder(folder)
    batchParam.setFileName(fileName)
    batchParam.setFormat(format)
    val resolutionTMS: Double = 156543.033928
    level = if (scale != null && scale.nonEmpty) {
      batchParam.setScale(scale)
      Math.floor(Math.log(resolutionTMS / scale.toDouble) / Math.log(2)).toInt + 1
    } else {
      batchParam.setScale("-1")
      -1
    }
    if (sc.master.contains("local")) {
      JsonToArg.jsonAlgorithms = GlobalConfig.Others.jsonAlgorithms
    }
    JsonToArg.trans(dagJsonObject, "0")
    println("JsonToArg.dagMap.size = " + JsonToArg.dagMap)
    JsonToArg.dagMap.foreach(DAGList => {
      println("************优化前的DAG*************")
      println(DAGList._1)
      DAGList._2.foreach(println(_))
      println("************优化后的DAG*************")
      val optimizedDAGList: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = optimizedDAG(DAGList._2)
      optimizedDagMap += (DAGList._1 -> optimizedDAGList)
      optimizedDAGList.foreach(println(_))
    })
    try {
      lambda(sc, optimizedDagMap("0"))
      PostSender.sendShelvedPost()
    } catch {
      case e: Throwable =>
        val errorJson = new JSONObject
        errorJson.put("error", e.toString)
        val outJsonObject: JSONObject = new JSONObject
        outJsonObject.put("workID", Trigger.dagId)
        outJsonObject.put("json", errorJson)
        sendPost(DAG_ROOT_URL + "/deliverUrl", outJsonObject.toJSONString)
        logger.error(s"Executing batch task failed! Error JSON: $outJsonObject", e)
    } finally {
      val tempFilePath = GlobalConfig.Others.tempFilePath
      val filePath = s"${tempFilePath}${dagId}.tiff"
      tempFileList.foreach(file => {
        if (scala.reflect.io.File(file).exists)
          scala.reflect.io.File(file).delete()
      })
      tempFileList.clear()
      Trigger.outputInformationList.clear()
      Trigger.optimizedDagMap.clear()
      Trigger.coverageCollectionMetadata.clear()
      Trigger.lazyFunc.clear()
      Trigger.coverageCollectionRddList.clear()
      Trigger.coverageRddList.clear()
      Trigger.zIndexStrArray.clear()
      JsonToArg.dagMap.clear()
      Trigger.tableRddList.clear()
      Trigger.kernelRddList.clear()
      Trigger.mlmodelRddList.clear()
      Trigger.featureRddList.clear()
      Trigger.cubeRDDList.clear()
      Trigger.cubeLoad.clear()
      Trigger.intList.clear()
      Trigger.doubleList.clear()
      Trigger.stringList.clear()
      PostSender.clearShelvedMessages()
    }
  }
}