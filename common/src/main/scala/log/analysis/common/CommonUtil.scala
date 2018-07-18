package log.analysis.common

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.io.Source

object CommonUtil {
  val LOGGER = LoggerFactory.getLogger(CommonUtil.getClass)

  case class DomainInfo(userId: Int, domainId: Int, userDomainId: String)

  var lastDomainConf = ""
  var domainTableBroadcast: Broadcast[Map[String, DomainInfo]] = null

  def genDomainInfoBroadCast(sparkContext: SparkContext): Broadcast[Map[String, DomainInfo]] = {


    //    LOGGER.info("Enter genDomainInfoBroadCast")
    val domainInfoList = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).listStatus(new Path("/cdn/domaininfo/")).sortBy(fileStatus => fileStatus.getModificationTime)(Ordering[Long].reverse)
    if (domainInfoList == null || domainInfoList.isEmpty) {
      LOGGER.warn("no domain file in obs")
      null
    }
    val domainInfoFile = domainInfoList(0).getPath
    if (!lastDomainConf.equals(domainInfoFile.getName)) {
      LOGGER.info("parse domain info file " + domainInfoFile.getName)
      if (domainTableBroadcast != null) domainTableBroadcast.unpersist()
      lastDomainConf = domainInfoFile.getName
      val domainTableSource = Source.fromInputStream(FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).open(domainInfoFile), "UTF-8")
      val domainTable = domainTableSource.getLines().toArray.map(line => {
        val Array(domainId, userId, host, userDomainId, createTime, deleteTime) = line.split("#")
        try {
          if (deleteTime.equals("0")) {
            (host, DomainInfo(userId.toInt, domainId.toInt, userDomainId))
          } else {
            null
          }
        }
        catch {
          case ex: Throwable =>
            LOGGER.error("domain parse fail " + line, ex)
            null
        }
      }).filter(x => x != null).toMap
      domainTableBroadcast = sparkContext.broadcast(domainTable)
    }
    domainTableBroadcast
  }
}
