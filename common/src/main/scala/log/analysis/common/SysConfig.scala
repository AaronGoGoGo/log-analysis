package log.analysisn.common

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object SysConfig {


  var prop = new Properties()

  init("/cdn/config/job.properties")

  val OBS_ENDPOINT: String = get("obs.endpoint")
  val OBS_AK: String = get("obs.ak")
  val OBS_SK: String = get("obs.sk")
  val JOB_5MIN_PARTITION_SIZE: Int = get("job.5min.partition.size").toInt
  val JOB_5MIN_INPUT_SIZE: Long = get("job.5min.input.size").toLong
  val JOB_5MIN_INPUT_COMPRESS_SIZE: Long = get("job.5min.input.compress.size").toLong
  val JOB_1Hour_PARTITION_SIZE: Int = get("job.1Hour.partition.size").toInt * 1024 * 1024

  val RAW_DATA_BUCKET: String = get("bucket.raw")
  val TENCENT_RAW_DATA_BUCKET: String = get("bucket.raw.tencent")
  val TENCENT_OVERSEA_RAW_DATA_BUCKET: String = get("bucket.raw.tencent.oversea")
  val CNC_RAW_DATA_BUCKET: String = get("bucket.raw.cnc")
  val CNC_OVERSEA_RAW_DATA_BUCKET: String = get("bucket.raw.cnc.oversea")
  val OUTPUT_DATA_BUCKET: String = get("bucket.output")
  val MIDDLE_DATA_BUCKET: String = get("bucket.middle")
  val JDBC_ISP_ENDPOINT : String = get("jdbc.isp.endpoint")
  val JDBC_ISP_USER : String = get("jdbc.isp.user")
  val JDBC_ISP_PASSWORD : String = WccUtil.getInstance().decrypt(get("jdbc.isp.password"))

  val JDBC_URL_ENDPOINT : String = get("jdbc.url.endpoint")
  val JDBC_URL_USER : String = get("jdbc.url.user")
  val JDBC_URL_PASSWORD : String = WccUtil.getInstance().decrypt(get("jdbc.url.password"))

  val MANGODOMAIN = List("www.baidu.com", "www.sina.com")

  val RAW_DATA_DIR = "/"
  val RAW_DATA_DONE_DIR = "/Raw_File_DONE/"

  val RAW_DATA_FAIL_DIR = "/Raw_File_FAIL/"

  val URL_TEMP_DIR = "/cdn/urltemp/"

  val URL_TEMP_DIR_OVERSEA = "/cdn/urltemp_oversea/"

  val MERGED_DIR = "/cdn/5Min/"

  val MERGED_DIR_OVERSEA = "/cdn/5Min_oversea/"

  val URL_STAGE_NEW = "new"

  val URL_STAGE_MERGED = "merged"

  val TENCENT = "TENCENT"

  val CHINANETCENTER = "CHINANETCENTER"

  val UCDN = "UCDN"

  val URLSPLIT = " "

  def get(key : String) : String = {
    prop.getProperty(key)
  }
  private def init(path: String): Unit =
  {
    prop = loadProperties(path)
  }

  private def loadProperties(path : String) : Properties = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    val file = fs.open(new Path(path))
    val propp = new Properties()
    try {
      propp.load(file)
    }
    catch
  {
    case e: Exception => println("load properties error ", e)
  }
    finally {
     IOUtils.closeStream(file)
    }
    propp
  }
}
