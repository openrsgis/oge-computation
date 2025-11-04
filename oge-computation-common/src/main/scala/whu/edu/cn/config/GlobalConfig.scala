package whu.edu.cn.config


import com.typesafe.scalalogging.StrictLogging
import lombok.extern.slf4j.Slf4j
import org.apache.commons.lang3.StringUtils
import pureconfig.module.yaml
import pureconfig.generic.auto._
import whu.edu.cn.util.{BeanUtils, ClasspathUtils}

import java.io.{File, FileInputStream}

@Slf4j
object GlobalConfig extends StrictLogging {

  var appConfig: Config = null

  /**
   * 静态加载配置
   */
  {
    loadConfig()
  }

  // 基础配置类
  case class DagBootYamlConf(dagRootUrl: String = null)

  case class RedisYamlConf(
                            jedisHost: String = null,
                            jedisPort: Int = -1,
                            jedisPwd: String = null,
                            redisCacheTtl: Int = -1
                          )

  case class StorageYamlConf(
                              endpoint: String = null,
                              accessKey: String = null,
                              secretKey: String = null,
                              bucketName: String = null,
                              headSize: Int = -1,
                              maxConnections: Int = -1,
                              storageType: String = null
                            )

  case class PostgresqlYamlConf(
                                 postgresqlUrl: String = null,
                                 postgresqlDriver: String = null,
                                 postgresqlUser: String = null,
                                 postgresqlPwd: String = null,
                                 postgresqlMaxRetries: Int = -1,
                                 postgresqlRetryDelay: Int = -1
                               )

  case class GcYamlConf(
                         localDataRoot: String = null,
                         httpDataRoot: String = null,
                         localHtmlRoot: String = null,
                         algorithmJson: String = null
                       )

  case class QgisYamlConf(
                           qgisData: String = null,
                           qgisAlgorithmCode: String = null,
                           qgisHost: String = null,
                           qgisUsername: String = null,
                           qgisPassword: String = null,
                           qgisPort: Int = -1,
                           qgisPython: String = null,
                           qgisRs: String = null
                         )

  case class otbYamlConf(
                          otbData: String = null,
                          otbAlgorithmCode: String = null,
                          otbDockerData: String = null,
                          otbHost: String = null,
                          otbUsername: String = null,
                          otbPassword: String = null,
                          otbPort: Int = -1
                        )

  case class SAGAYamlConf(
                           sagaData: String = null,
                           sagaDockerData: String = null,
                           sagaHost: String = null,
                           sagaUsername: String = null,
                           sagaPassword: String = null,
                           sagaPort: Int = -1
                         )

  case class QuantYamlConf(
                            quantDataPath: String = null,
                            quantACPath: String = null,
                            quantHost: String = null,
                            quantUsername: String = null,
                            quantPassword: String = null,
                            quantPort: Int = -1
                          )

  case class ThirdApplicationYamlConf(
                                       thirdHost: String = null,
                                       thirdUsername: String = null,
                                       thirdPassword: String = null,
                                       dockerData: String = null,
                                       serverData: String = null,
                                       thirdPort: Int = -1
                                     )

  case class DockerSwarmYamlConf(
                                  masterHost: String = null,
                                  registryPort: Int = -1,
                                  constraint: String = null,
                                  mode: String = null,
                                  mountType: String = null,
                                  mountSource: String = null,
                                  mountTarget: String = null,
                                  algorithmJson: String = null,
                                  hubServer: String = null,
                                  constraintCpu: String = null,
                                  constraintGpu: String = null
                                ) {
  }

  case class ServiceYamlConf(baseUrl: String = null, thirdSource: String = null)

  case class OthersConf(
                         thirdJson: String = null,
                         jsonAlgorithms: String = null,
                         tempFilePath: String = null,
                         sagaTempFilePath: String = null,
                         otbTempFilePath: String = null,
                         tmsPath: String = null,
                         tmsHost: String = null,
                         tomcatHost: String = null,
                         tomcatHostPublic: String = null,
                         onTheFlyStorage: String = null,
                         jsonSavePath: String = null,
                         platform: String = null,
                         hbaseHost: String = null,
                         thresholdRetile: Int = 50
                       )


  // 全局配置类
  case class Config(
                     dagBootConf: DagBootYamlConf = null,
                     redisConf: RedisYamlConf = null,
                     storageConf: StorageYamlConf = null,
                     postgresqlConf: PostgresqlYamlConf = null,
                     gcConf: GcYamlConf = null,
                     qgisConf: QgisYamlConf = null,
                     otbConf: otbYamlConf = null,
                     saga: SAGAYamlConf = null,
                     quant: QuantYamlConf = null,
                     thirdApplication: ThirdApplicationYamlConf = null,
                     dockerSwarm: DockerSwarmYamlConf = null,
                     service: ServiceYamlConf = null,
                     others: OthersConf = null
                   )

  /**
   * 从配置文件与环境变量加载配置，加载顺序：
   * 1，读取config.yaml文件
   * 2，若PROFILE环境变量存在，则继续加载config-${PROFILE}.yaml文件，并将其中非空且与config.yaml值不同的字段覆盖
   * 3，最后，从环境变量读取配置，如果某一配置类字段对应的环境变量名存在，则使用该环境变量值覆盖该配置，具体规则和逻辑参考[[BeanUtils]]的environmentVariableInject方法
   * 综上所述，对于同一配置字段，其优先级为：环境变量 > config-${PROFILE}.yaml > config.yaml
   */
  def loadConfig(): Unit = {
    try {
      // 1.加载默认配置（JAR包内）
      val yamlContent = ClasspathUtils.readAsString("/config.yaml", getClass)
      appConfig = yaml.loadYamlOrThrow[Config](yamlContent)
      // 2.加载环境特定配置（优先尝试JAR包内，再尝试外部文件）
      val profile = System.getenv("PROFILE")
      if (!StringUtils.isEmpty(profile)) {
        val overridePath = s"config-$profile.yaml"
        // 优先尝试外部文件
        val overrideStream = Option(new File(overridePath))
          .filter(_.exists()) // 检查文件是否存在
          .map(f => new FileInputStream(f)) // 存在则打开流
          // 若外部文件不存在，回退到JAR内部资源
          .orElse(Option(ClasspathUtils.readAsStream(s"/$overridePath", getClass)))
          // 最终过滤空值
          .filter(_ != null)
        overrideStream.foreach { stream =>
          val overrideContent = scala.io.Source.fromInputStream(stream).mkString
          val configOverride = yaml.loadYamlOrThrow[Config](overrideContent)
          // 合并配置逻辑
          BeanUtils.mergeConfig(appConfig, configOverride)
        }
      }
      // 3.从环境变量注入（OGE_XXX_XXX）
      BeanUtils.environmentVariableInject(appConfig, prefix = "oge")
    } catch {
      case e: Exception =>
        logger.error("加载配置失败", e)
    }
  }

  object DagBootConf {
    // dag-boot 服务根路径
    var DAG_ROOT_URL: String = appConfig.dagBootConf.dagRootUrl
    var EDU_ROOT_URL: String = appConfig.dagBootConf.dagRootUrl
  }

  object RedisConf {
    // Redis 基础配置
    final val JEDIS_HOST: String = appConfig.redisConf.jedisHost
    final val JEDIS_PORT: Int = appConfig.redisConf.jedisPort
    final val JEDIS_PWD: String = appConfig.redisConf.jedisPwd
    // Redis 超时时间
    final val REDIS_CACHE_TTL: Long = 2 * 60L
  }

  object ClientConf {
    final val CLIENT_NAME: String = appConfig.storageConf.storageType
    final val USER_BUCKET_NAME: String = "oge-user"
  }

  object MinioConf {
    // MinIO 基础配置
    final val MINIO_ENDPOINT: String = appConfig.storageConf.endpoint
    final val MINIO_ACCESS_KEY: String = appConfig.storageConf.accessKey
    final val MINIO_SECRET_KEY: String = appConfig.storageConf.secretKey
    final val MINIO_BUCKET_NAME: String = appConfig.storageConf.bucketName
    final val MINIO_HEAD_SIZE: Int = appConfig.storageConf.headSize
    final val MINIO_MAX_CONNECTIONS: Int = appConfig.storageConf.maxConnections
  }

  object BosConf {
    //Bos基础配置
    final val BOS_ACCESS_KEY: String = appConfig.storageConf.accessKey
    final val BOS_SECRET_ACCESS: String = appConfig.storageConf.secretKey
    final val BOS_ENDPOINT: String = appConfig.storageConf.endpoint
    final val BOS_BUCKET_NAME: String = appConfig.storageConf.bucketName
  }

  object PostgreSqlConf {
    // PostgreSQL 基础配置
    var POSTGRESQL_URL: String = appConfig.postgresqlConf.postgresqlUrl
    var POSTGRESQL_DRIVER: String = appConfig.postgresqlConf.postgresqlDriver
    var POSTGRESQL_USER: String = appConfig.postgresqlConf.postgresqlUser
    var POSTGRESQL_PWD: String = appConfig.postgresqlConf.postgresqlPwd
    var POSTGRESQL_MAX_RETRIES: Int = appConfig.postgresqlConf.postgresqlMaxRetries
    var POSTGRESQL_RETRY_DELAY: Int = appConfig.postgresqlConf.postgresqlRetryDelay
  }

  // GcConst
  object GcConf {
    final val localDataRoot = appConfig.gcConf.localDataRoot
    final val httpDataRoot = appConfig.gcConf.httpDataRoot
    final val localHtmlRoot = appConfig.gcConf.localHtmlRoot
    final val algorithmJson = appConfig.gcConf.algorithmJson


    //landsat-8 pixel value in BQA band from USGS
    final val cloudValueLs8: Array[Int] = Array(2800, 2804, 2808, 2812, 6896, 6900, 6904, 6908)
    final val cloudShadowValueLs8: Array[Int] = Array(2976, 2980, 2984, 2988, 3008, 3012, 3016, 3020, 7072, 7076,
      7080, 7084, 7104, 7108, 7112, 7116)
  }

  object QGISConf {
    // PostgreSQL 基础配置
    var QGIS_DATA: String = appConfig.qgisConf.qgisData
    var QGIS_ALGORITHMCODE: String = appConfig.qgisConf.qgisAlgorithmCode
    var QGIS_HOST: String = appConfig.qgisConf.qgisHost
    var QGIS_USERNAME: String = appConfig.qgisConf.qgisUsername
    var QGIS_PASSWORD: String = appConfig.qgisConf.qgisPassword
    var QGIS_PORT: Int = appConfig.qgisConf.qgisPort
  }

  object OTBConf {
    var OTB_DATA: String = appConfig.otbConf.otbData
    var OTB_ALGORITHMCODE: String = appConfig.otbConf.otbAlgorithmCode
    var OTB_DOCKERDATA: String = appConfig.otbConf.otbDockerData
    var OTB_HOST: String = appConfig.otbConf.otbHost
    var OTB_USERNAME: String = appConfig.otbConf.otbUsername
    var OTB_PASSWORD: String = appConfig.otbConf.otbPassword
    var OTB_PORT: Int = appConfig.otbConf.otbPort
  }

  object SAGAConf {
    var SAGA_DATA: String = appConfig.saga.sagaData
    var SAGA_DOCKERDATA: String = appConfig.saga.sagaDockerData
    var SAGA_HOST: String = appConfig.saga.sagaHost
    var SAGA_USERNAME: String = appConfig.saga.sagaUsername
    var SAGA_PASSWORD: String = appConfig.saga.sagaPassword
    var SAGA_PORT: Int = appConfig.saga.sagaPort
  }

  object QuantConf {
    var Quant_DataPath: String = appConfig.quant.quantDataPath
    var Quant_ACpath: String = appConfig.quant.quantACPath
    var Quant_HOST: String = appConfig.quant.quantHost
    var Quant_USERNAME: String = appConfig.quant.quantUsername
    var Quant_PASSWORD: String = appConfig.quant.quantPassword
    var Quant_PORT: Int = appConfig.quant.quantPort
  }

  object ThirdApplication {
    var THIRD_HOST: String = appConfig.thirdApplication.thirdHost
    var THIRD_USERNAME: String = appConfig.thirdApplication.thirdUsername
    var THIRD_PASSWORD: String = appConfig.thirdApplication.thirdPassword
    final var DOCKER_DATA: String = appConfig.thirdApplication.dockerData
    final var SERVER_DATA: String = appConfig.thirdApplication.serverData
    final var THIRD_PORT: Int = appConfig.thirdApplication.thirdPort
  }

  object DockerSwarmConf {
    final var MASTER_HOST: String = appConfig.dockerSwarm.masterHost
    final var REGISTRY_PORT: Int = appConfig.dockerSwarm.registryPort
    final var CONSTRAINT: String = appConfig.dockerSwarm.constraint
    final var MODE: String = appConfig.dockerSwarm.mode
    final var MOUNT_TYPE: String = appConfig.dockerSwarm.mountType
    final var MOUNT_SOURCE: String = appConfig.dockerSwarm.mountSource
    final var MOUNT_TARGET: String = appConfig.dockerSwarm.mountTarget
    final var ALGORITHM_JSON: String = appConfig.dockerSwarm.algorithmJson
    final var HUB_SERVER: String = appConfig.dockerSwarm.hubServer
    final var CONSTRAINT_CPU: String = appConfig.dockerSwarm.constraintCpu
    final var CONSTRAINT_GPU: String = appConfig.dockerSwarm.constraintGpu

  }

  object ServiceConf {
    val BASE_URL: String = appConfig.service.baseUrl
    val THIRD_SOURCE: String = appConfig.service.thirdSource
  }

  object Others {
    final var thirdJson = appConfig.others.thirdJson
    final var jsonAlgorithms = appConfig.others.jsonAlgorithms
    final var tempFilePath = appConfig.others.tempFilePath
    final var sagaTempFilePath = appConfig.others.sagaTempFilePath
    final var otbTempFilePath = appConfig.others.otbTempFilePath
    final var tmsPath = appConfig.others.tmsPath
    final var tmsHost = appConfig.others.tmsHost
    final var tomcatHost = appConfig.others.tomcatHost
    final var tomcatHost_public = appConfig.others.tomcatHostPublic
    final var onTheFlyStorage = appConfig.others.onTheFlyStorage
    final var jsonSavePath = appConfig.others.jsonSavePath
    final var bucketName = appConfig.storageConf.bucketName
    var platform = appConfig.others.platform
    final var hbaseHost = appConfig.others.hbaseHost
    final var thresholdRetile = appConfig.others.thresholdRetile
  }
}