package log.analysis.topurl

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import log.analysis.common.CommonUtil.DomainInfo
import log.analysis.common.{CommonUtil, FileSystemFactory, SysConfig, WccUtil}
import log.analysis.topurl.util.DBWriter
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object TopUrlJob {
  val LOGGER = LoggerFactory.getLogger(TopUrlJob.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.set("spark.hadoop.fs.s3a.endpoint", SysConfig.OBS_ENDPOINT)
    conf.set("spark.hadoop.fs.s3a.access.key", SysConfig.OBS_AK)
    conf.set("spark.hadoop.fs.s3a.secret.key", WccUtil.getInstance().decrypt(SysConfig.OBS_SK))
    conf.setAppName("TopUrlJob")
    val obsfs = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET)
    val hdfsfs = FileSystemFactory.getHdfsFs
    var count = 100
    val processStart = new Date().getTime
    val sparkContext = new SparkContext(conf)



    LOGGER.info("start topurl Job")
    while (count > 0) {
      if (new Date().getTime - processStart > 24 * 3600 * 1000) {
        LOGGER.info("reach specified limit time.")
        sparkContext.stop()
        return
      }

      if (hdfsfs.exists(new Path("/cdn/config/TopUrlStop.md"))) {
        LOGGER.info("find stop label.")
        hdfsfs.delete(new Path("/cdn/config/TopUrlStop.md"), true)
        sparkContext.stop()
        return
      }

      val domainTableBroadcast = CommonUtil.genDomainInfoBroadCast(sparkContext)
      if (null == domainTableBroadcast) {
        LOGGER.info("domainTableBroadcast is null")
        return
      }
      val metaDir = "/cdn/meta/TopUrlJob/"

      try {
        if (!hdfsfs.exists(new Path(metaDir))) hdfsfs.mkdirs(new Path(metaDir))
        val jobs = hdfsfs.listStatus(new Path(metaDir)).map(fileStatus => fileStatus.getPath.getName)
          .sortBy(x => x)

        if (jobs.length > 0) {
          //处理指定的任务数
          //          count -= 1
          val currentJob = jobs(0)
          LOGGER.info("----------------------------- deal job " + currentJob)
          val jobStart = new Date().getTime

          handleOneJob(obsfs, sparkContext, currentJob, domainTableBroadcast, isOverSea = false)

          handleOneJob(obsfs, sparkContext, currentJob, domainTableBroadcast, isOverSea = true)

          hdfsfs.delete(new Path(metaDir + currentJob), true)

          LOGGER.info("---------------- job take time " + (new Date().getTime - jobStart))
        }
        else {
          LOGGER.debug("---------no job to deal with------------")
        }
        LOGGER.debug("-----------------sleep 300000 ms--------------")
        Thread.sleep(10)
      }
      catch {
        case ex: Throwable => {
          LOGGER.error("spark exception ", ex)
          sparkContext.stop()
          return
        }
      }
    }
    sparkContext.stop()
  }

  private def handleOneJob(obsfs: FileSystem,
                           sparkContext: SparkContext,
                           currentJob: String,
                           domainTableBroadcast: Broadcast[Map[String, DomainInfo]],
                           isOverSea : Boolean): Unit = {
    val tempUrlDir = if (isOverSea) SysConfig.URL_TEMP_DIR_OVERSEA else SysConfig.URL_TEMP_DIR
    val currentDir = tempUrlDir + currentJob + "/"
    if (!obsfs.exists(new Path(currentDir))) {
      return
    }
    val allFile = obsfs.listStatus(new Path(tempUrlDir + currentJob + "/"))
    val allFreshDomain = allFile.filter(fileStatus => fileStatus.getPath.getName.split("#")(0).equals(SysConfig.URL_STAGE_NEW)).
      map(fileStatus => fileStatus.getPath.getName.split("#")(1)).groupBy(x => x).keys.toSet
    var allDomainStat = allFreshDomain.map(x => (x,0L)).toMap

    val allDealFiles = allFile.filter(fileStatus => {
      val domain = fileStatus.getPath.getName.split("#")(1)
      allDomainStat = allDomainStat.map(x => {
        if (x._1.equals(domain)) {
          (x._1,x._2 + fileStatus.getLen)
        } else {
          x
        }
      })
      if ( allFreshDomain.contains(domain))
      {
        allDomainStat(domain) < 200 * 1024 * 1024
      }
      else {
        false
      }
    }).map(fileStatus => fileStatus.getPath.getParent + "/" + fileStatus.getPath.getName)
    LOGGER.info("-------------------------- deal File num " + allDealFiles.length)
    //          Thread.sleep(1000)
    val partition = allFreshDomain.size
    val format = new SimpleDateFormat("yyyyMMdd")
    val point = format.parse(currentJob).getTime
    if (allDealFiles.nonEmpty) {
      println("+++++++++++++" + allDealFiles.mkString(","))
      sparkContext.textFile(allDealFiles.mkString(",")).map(x => {
        val sArray = x.split(SysConfig.URLSPLIT)
        val host = sArray(0)
        val uri = sArray(1)
        val count = sArray(2)
        val traffic = sArray(3)
        domainTableBroadcast.value.get(host) match {
          case Some(a) => ((host, uri, a.userId, a.domainId), (count.toLong, traffic.toLong))
          case _ => (null, null)
        }
      }).filter(x => x._1 != null).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2)).map(x => ((x._1._1, x._1._3, x._1._4), (x._1._2, x._2._1, x._2._2)))
        .groupByKey(partition).foreach(x => {
        val dayTime = currentJob
        val host = x._1._1
        val userId = x._1._2
        val hostId = x._1._3
        val fileName = (SysConfig.URL_STAGE_MERGED,host,UUID.randomUUID()).productIterator.mkString("#") + ".txt"
        val fileNamePath = tempUrlDir + dayTime + "/" + fileName
        val stream = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(fileNamePath))
        val writer = new PrintWriter(stream)
        val topUrls = (x._2.toList.sortWith((x1,x2) => x1._2 > x2._2).take(100000) ++ x._2.toList.sortWith((x1,x2) => x1._3 > x2._3).take(100000)).distinct
        topUrls.map(x => host + SysConfig.URLSPLIT + x.productIterator.mkString(SysConfig.URLSPLIT)).grouped(1000).foreach(x => {
          writer.write(x.mkString("\n") + "\n")
        })
        writer.close()
        val countTop = topUrls.map(x => (x._1, x._2)).toArray.sortWith((x1, x2) => x1._2 > x2._2).take(200)
        DBWriter.writeTopAccessUrl(userId, hostId, point, countTop,isOverSea)
        val trafficTop = topUrls.map(x => (x._1, x._3)).toArray.sortWith((x1, x2) => x1._2 > x2._2).take(200)
        DBWriter.writeTopFluxUrl(userId, hostId, point, trafficTop,isOverSea)
       // (host, countTop.map(x => x.productIterator.mkString("|")).mkString(","), trafficTop.map(x => x.productIterator.mkString("|")).mkString(","))
      })//.collect().foreach(x => println("+++++++++" + x.productIterator.mkString(",")))

      sparkContext.parallelize(allDealFiles.toSeq).foreach(file => FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).delete(new Path(file), true))


    }
  }
}