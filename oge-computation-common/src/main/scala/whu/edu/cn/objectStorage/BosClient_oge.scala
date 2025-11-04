package whu.edu.cn.objectStorage

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.model.GetObjectRequest
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}

import java.io.{File, InputStream}

class BosClient_oge(endPoint: String) extends ObjectStorageClient {
  val ACCESS_KEY_ID = ""
  val SECRET_ACCESS_ID = ""
  val client = getClient

  private def getClient = {
    val config = new BosClientConfiguration
    config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_ID))
    config.setEndpoint(endPoint)
    val client_1 = new BosClient(config)
    client_1
  }


  override def getInputStream(bucketName: String, path: String, range: Int): InputStream = {
    val getObjectRequest: GetObjectRequest = new GetObjectRequest(bucketName, path)
    getObjectRequest.setRange(0, range)
    val bucketObj = client.getObject(getObjectRequest)
    bucketObj.getObjectContent()
  }

  override def DownloadObject(path: String, filepath: String, bucketName: String): Unit = {
    val file = new File(filepath)
    client.getObject(bucketName, path, file)
  }

  override def UploadObject(path: String, filepath: String, bucketName: String): Unit = {
    val file = new File(filepath)
    client.putObject(bucketName, path, file)
  }
}