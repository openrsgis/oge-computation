package whu.edu.cn.util

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.model.{BosObject, GetObjectRequest}
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}
import io.minio.{GetObjectArgs, MinioClient, UploadObjectArgs}
import whu.edu.cn.config.GlobalConfig.BosConf.{BOS_ACCESS_KEY, BOS_ENDPOINT, BOS_SECRET_ACCESS}
import whu.edu.cn.config.GlobalConfig.ClientConf.USER_BUCKET_NAME
import whu.edu.cn.config.GlobalConfig.MinioConf.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.trigger.Trigger.tempFileList

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.concurrent.LinkedBlockingQueue

abstract class ClientUtil extends Serializable {

  val pool: LinkedBlockingQueue[Any] = new LinkedBlockingQueue(50)

  def getClient: Any

  def getClientPool: Any

  def returnClient(client: Any): Unit

  def getObject(bucketName: String, path: String): InputStream

  def Download(objectName: String, downloadPath: String): Unit

  def Upload(objectName: String, filePath: String): Unit

  def UploadDir(objectName: String, filePath: String): Unit


  def getBucketAndPath(coverageId: String, userID: String): (String, String) = {
    if (coverageId.startsWith("file:")) {
      val coverageValue = coverageId.replace("file:", "")
      val bucketName = coverageValue.split("/")(0)
      var path = coverageValue.replace(bucketName + "/", "")
      if (!(path.endsWith(".tiff") || path.endsWith(".tif"))) {
        path = s"$path.tiff"
      }
      (bucketName, path)
    } else {
      var path: String = new String()
      if (coverageId.endsWith(".tiff") || coverageId.endsWith(".tif")) {
        path = s"${userID}/$coverageId"
      } else {
        path = s"$userID/$coverageId.tiff"
      }
      (USER_BUCKET_NAME, path)
    }
  }

  def resolveBucketName(bucketName: String, defualtBucketName: String = USER_BUCKET_NAME): String = {
    Option(bucketName).filter(_.nonEmpty).getOrElse(defualtBucketName)
  }
}

class MinioClientUtil extends ClientUtil {
  // transient: 序列化时忽略此字段
  // volatile: 确保多线程环境下的可见性（当一个线程修改了 instance，其他线程能立即看到）
  @transient
  @volatile private var instance: MinioClient = null

  override def getClient: Any = {
    if (instance == null) {
      instance = MinioClient.builder()
        .endpoint(MINIO_ENDPOINT)
        .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        .build()
      instance.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
      instance
    } else {
      instance
    }
  }

  override def getClientPool: Any = {
    pool.poll() match {
      case null => // 池为空时创建新客户端
        val instance = MinioClient.builder()
          .endpoint(MINIO_ENDPOINT)
          .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
          .build()
        instance.setTimeout(10 * 60 * 1000, 10 * 60 * 1000, 10 * 60 * 1000)
        instance
      case client =>
        client // 复用池中的客户端
    }
  }

  override def returnClient(client: Any): Unit = {
    if (!pool.offer(client)) {
      println("连接池已满，关闭并丢弃客户端")
    }
  }

  override def getObject(bucketName: String, path: String): InputStream = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    minioClient.getObject(GetObjectArgs.builder.bucket(bucketName).`object`(path).build())
  }

  override def Download(objectName: String, downloadPath: String): Unit = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    val filePath = downloadPath
    val inputStream = minioClient.getObject(GetObjectArgs.builder.bucket(USER_BUCKET_NAME).`object`(objectName).build())
    val outputPath = Paths.get(filePath)
    tempFileList.append(filePath)
    Trigger.file_id += 1
    java.nio.file.Files.copy(inputStream, outputPath, REPLACE_EXISTING)
    inputStream.close()
  }

  override def Upload(objectName: String, filePath: String): Unit = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    minioClient.uploadObject(UploadObjectArgs.builder.bucket(USER_BUCKET_NAME).`object`(objectName).filename(filePath).build())
  }

  override def UploadDir(objectName: String, filePath: String): Unit = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    minioClient.uploadObject(UploadObjectArgs.builder.bucket(USER_BUCKET_NAME).`object`(objectName).filename(filePath).build())
    val file: File = new File(filePath)
    file.list().foreach(t => minioClient.uploadObject(UploadObjectArgs.builder
      .bucket(USER_BUCKET_NAME).
      `object`(objectName + "/" + t).
      filename(filePath + "/" + t).
      build()))
  }
}

class BosClientUtil extends ClientUtil {
  // transient: 序列化时忽略此字段
  // volatile: 确保多线程环境下的可见性（当一个线程修改了 instance，其他线程能立即看到）
  @transient
  @volatile private var instance: BosClient = null

  override def getClient: Any = {
    if (instance == null) {
      val config = new BosClientConfiguration
      config.setCredentials(new DefaultBceCredentials(BOS_ACCESS_KEY, BOS_SECRET_ACCESS))
      config.setEndpoint(BOS_ENDPOINT)
      //      config.setMaxRedirects(8)
      // 服务器建立连接时愿意等待的最长时间
      config.setConnectionTimeoutInMillis(20 * 1000)
      //指连接建立后，客户端等待服务器返回数据的最长时间
      config.setSocketTimeoutInMillis(30 * 1000)
      instance = new BosClient(config)
      instance
    } else {
      instance
    }
  }

  override def getClientPool: Any = {
    pool.poll() match {
      case null => // 池为空时创建新客户端
        val config = new BosClientConfiguration
        config.setCredentials(new DefaultBceCredentials(BOS_ACCESS_KEY, BOS_SECRET_ACCESS))
        config.setEndpoint(BOS_ENDPOINT)
        config.setMaxRedirects(8)
        // 服务器建立连接时愿意等待的最长时间
        config.setConnectionTimeoutInMillis(20000)
        //指连接建立后，客户端等待服务器返回数据的最长时间
        config.setSocketTimeoutInMillis(40000)
        new BosClient(config)
      case client => client.asInstanceOf[BosClient] // 复用池中的客户端
    }
  }

  override def returnClient(client: Any): Unit = {
    if (!pool.offer(client)) {
      println("连接池已满，丢弃归还的客户端")
    }
  }

  override def getObject(bucketName: String, path: String): InputStream = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val getObjectRequest: GetObjectRequest = new GetObjectRequest(bucketName, path)
    val bucketObject: BosObject = bosClient.getObject(getObjectRequest)
    bucketObject.getObjectContent()
  }

  override def Download(objectName: String, downloadPath: String): Unit = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val getObjectRequest = new GetObjectRequest(USER_BUCKET_NAME, objectName)
    val tempfile = new File(downloadPath)
    tempfile.createNewFile()
    val bosObject = bosClient.getObject(getObjectRequest, tempfile)
  }

  override def Upload(objectName: String, filePath: String): Unit = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val file: File = new File(filePath)
    bosClient.putObject(USER_BUCKET_NAME, objectName, file)
  }

  override def UploadDir(objectName: String, filePath: String): Unit = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val file: File = new File(filePath)
    file.list().foreach(t => {
      bosClient.putObject(USER_BUCKET_NAME,
        objectName + "/" + t,
        new File(filePath + "/" + t))
    })
  }
}

object ClientUtil {
  def createClientUtil(serviceType: String): ClientUtil = {
    serviceType.toLowerCase match {
      case "minio" => new MinioClientUtil()
      case "bos" => new BosClientUtil()
      case _ => throw new IllegalArgumentException("Invalid service type")
    }
  }
}
