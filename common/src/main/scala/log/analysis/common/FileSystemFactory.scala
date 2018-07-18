package log.analysis.common

import java.net.URI

import org.apache.hadoop.fs.FileSystem

object FileSystemFactory {

  def getHdfsFs: FileSystem =
  {
    val fs = FileSystem.get(HadoopConfiguration.getConfig)
    fs
  }
  def getObfFs(bucket : String): FileSystem =
  {
    val obsFs = FileSystem.get(URI.create("s3a://" + bucket), HadoopConfiguration.getConfig)
    obsFs
  }
}
