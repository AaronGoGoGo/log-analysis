package log.analysis.common

import WccUtil
import org.apache.hadoop.conf.Configuration

object HadoopConfiguration {

  private val lock = null
  private var confInstance : Configuration = _
  def getConfig: Configuration =
  {
    synchronized
    {
      if (confInstance == null)
        {
          confInstance = new Configuration()
          confInstance.set("fs.s3a.endpoint",SysConfig.OBS_ENDPOINT)
          confInstance.set("fs.s3a.access.key", SysConfig.OBS_AK)
          confInstance.set("fs.s3a.secret.key", WccUtil.getInstance().decrypt(SysConfig.OBS_SK))
        }
    }
    confInstance
  }
}
