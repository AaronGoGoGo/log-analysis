package log.analysis.preprocess

import java.io.{BufferedInputStream, BufferedReader, InputStreamReader, PrintWriter}
import java.text.SimpleDateFormat
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.util.{Date, Locale, UUID}

import log.analysis.common.CommonUtil.DomainInfo
import log.analysis.common.{FileSystemFactory, SysConfig}
import log.analysis.preprocess.FiveMinuteJob.{LOGGER, covertedIpSegment, _}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.matching.Regex

object ThirdPartyExcutor {
  val LOGGER = LoggerFactory.getLogger(ThirdPartyExcutor.getClass)

  case class TencentLog(dateTime: String,
                        ip: String,
                        responseTime: String,
                        referer: String,
                        protocol: String,
                        method: String,
                        host: String,
                        uri: String,
                        httpCode: String,
                        size: String,
                        cacheState: String,
                        userAgent: String)

  def dealRawLog(sparkContext: SparkContext,
                 ipSrcBroadcast: Broadcast[Map[Int, Array[covertedIpSegment]]],
                 count: Int,
                 domainTableBroadcast: Broadcast[Map[String, DomainInfo]],
                 rawBucket: String,
                 vendor: String,
                 isOverSea: Boolean) = {
    val obsRawFs = FileSystemFactory.getObfFs(rawBucket)
    if (!obsRawFs.exists(new Path(SysConfig.RAW_DATA_DIR))) {
      obsRawFs.mkdirs(new Path(SysConfig.RAW_DATA_DIR))
    }
    var totalSize = 0L
    val dealFileArray = obsRawFs.listStatus(new Path(SysConfig.RAW_DATA_DIR)).filter(fileStatus => fileStatus.isFile && fileStatus.getPath.getName.matches(".*\\.gz") && fileStatus.getPath.getName.startsWith("fragment"))
      .takeWhile(fileStatus => {
        if (totalSize == 0) {
          totalSize += fileStatus.getLen
          true
        } else {
          totalSize += fileStatus.getLen
          LOGGER.info("----totalSize " + totalSize)
          totalSize < SysConfig.JOB_5MIN_INPUT_COMPRESS_SIZE * 1024 * 1024
        }
      })
      .map(fileStatus => fileStatus.getPath.getParent + "/" + fileStatus.getPath.getName)


    try {
      if (dealFileArray.length != 0) {
        LOGGER.info("dealFileArray size is " + dealFileArray.length)
        LOGGER.info(s"deal one source rawBucket $rawBucket, vendor : $vendor, isOverSea $isOverSea.")
        val start = new Date().getTime
        if (!isOverSea)
          handleFilesMainLand(sparkContext, ipSrcBroadcast, count, domainTableBroadcast, rawBucket, vendor, dealFileArray, totalSize.toInt)
        else
          handleFilesOverSea(sparkContext, ipSrcBroadcast, count, domainTableBroadcast, rawBucket, vendor, dealFileArray)
        LOGGER.info("---------------- job take time " + (new Date().getTime - start))
      }
    }
    catch {
      case ex: Throwable => LOGGER.error("test exception", ex)
        if (!FileSystemFactory.getObfFs(rawBucket).exists(new Path(SysConfig.RAW_DATA_FAIL_DIR))) {
          FileSystemFactory.getObfFs(rawBucket).mkdirs(new Path(SysConfig.RAW_DATA_FAIL_DIR))
        }
        sparkContext.parallelize(dealFileArray.toSeq).foreach(file => {
          val srcPath = new Path(file)
          val destPath = new Path(SysConfig.RAW_DATA_FAIL_DIR + srcPath.getName)
          if (!FileSystemFactory.getObfFs(rawBucket).rename(srcPath, destPath)) {
            FileSystemFactory.getObfFs(rawBucket).delete(srcPath, true)
          }
        })
        throw new Exception(ex)
    }
  }

  private def handleFilesMainLand(sparkContext: SparkContext,
                                  ipSrcBroadcast: Broadcast[Map[Int, Array[covertedIpSegment]]],
                                  count: Int,
                                  domainTableBroadcast: Broadcast[Map[String, DomainInfo]],
                                  rawBucket: String,
                                  vendor: String,
                                  dealFileArray: Array[String],
                                  totalSize: Int): Unit = {
    //首先将文件放入待合并目录，不需要定制格式
    LOGGER.info("total file  size is " + totalSize)
    val partition = totalSize / (20 * 1024 * 1024) + 1
    val rawRdd = sparkContext.textFile(dealFileArray.mkString(","))
    val cachedRdd = rawRdd.repartition(partition).map(line => {
      val logRegex: Regex = """\[(.+?)\] (.+?) (.+?) "(.+?)" "(.+?)" "(.+?)" "(.+?)" "(.+?)" (.+?) (.+?) (.+?) "(.+?)"""".r
      val tencentLog: TencentLog = logRegex.findFirstIn(line) match {
        case Some(logRegex(dateTime, ip, responseTime, referer, protocol, method, host, url, httpCode, size, cacheState, userAgent)) =>
          TencentLog(dateTime, ip, responseTime, referer, protocol, method, host, url.split("\\?")(0), httpCode, size, cacheState, userAgent)
        case _ => LOGGER.error("parse tencent log failed, line " + line)
          null
      }
      if (tencentLog != null) {
        val provinceOperator =
          try {
            getProvinceOperator(tencentLog.ip, ipSrcBroadcast)
          } catch {
            case e: Throwable =>
              LOGGER.error("line " + line, e)
              throw new Exception("line " + line, e)
          }
        val requestTime = getRequestTime(tencentLog.dateTime)
        val fiveMinPeriod = requestTime.getTime / 300000
        getHourTime(requestTime)
        val r = new Random()
        (fiveMinuteKey(tencentLog.host, getHourTime(requestTime), fiveMinPeriod), statUnit(tencentLog.uri, provinceOperator.province, provinceOperator.operator, tencentLog.size.toLong))
      } else {
        null
      }
    }).filter(x => x != null).groupByKey.map(x => {
      val statUnits = x._2.toArray
      val key = x._1
      //输出基于域名5分钟的URL临时统计值
      val uriStat = statUnits.map(statUnit => (statUnit.uri, statUnit.traffic)).groupBy(x => x._1).map(x => uriStatUnit(key.host, key.host + x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
      //输出基于域名输出5分钟的省份临时统计值
      val provinceStat = statUnits.map(statUnit => (statUnit.province, statUnit.traffic)).groupBy(x => x._1).map(x => provinceStatUnit(x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
      //输出基于域名输出5分钟的运营商临时统计值
      val operatorStat = statUnits.map(statUnit => (statUnit.operator, statUnit.traffic)).groupBy(x => x._1).map(x => operatorStatUnit(x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
      val poStat = statUnits.map(statUnit => (provinceAndOperator(statUnit.province, statUnit.operator), statUnit.traffic)).groupBy(x => x._1).map(x => ProvinceAndOperatorStatUnit(x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
      val host = key.host
      val domainInfo = domainTableBroadcast.value
      domainInfo.get(host) match {
        case Some(a) => (FiveMinStatKey(host, a.userId, a.domainId, key.hourTime, key.fiveMinPeriod), (uriStat, poStat))
        case _ =>
          val extensiveDomain = host.replace(host.split("\\.")(0), "*")
          domainInfo.get(extensiveDomain) match {
            case Some(a) => (FiveMinStatKey(extensiveDomain, a.userId, a.domainId, key.hourTime, key.fiveMinPeriod), (uriStat, poStat))
            case _ =>
              LOGGER.warn("domain is not match, will be filted. " + host)
              (null, null)
          }
      }
    }).filter(x => x._1 != null).persist(StorageLevel.DISK_ONLY)
    IspExcutor.dealIsp(cachedRdd.map(x => (x._1, x._2._2)), vendor)

    val tempUriRdd = cachedRdd.map(x => {
      val dayTime = x._1.hourTime.substring(0, 8)
      val host = x._1.host
      x._2._1.map(x => ((dayTime, host, x.uri), (x.count, x.traffic)))
    }).flatMap(x => x).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2)).map(x => {
      val dayTime = x._1._1
      val host = x._1._2
      val uri = x._1._3
      val count = x._2._1
      val traffic = x._2._2
      (uriKey(dayTime, host), (host, uri, count, traffic).productIterator.mkString(SysConfig.URLSPLIT))
    })

    //按照域名和天groupby
    val writerRdd = tempUriRdd.groupByKey()
    writerRdd.foreach(x => {
      val dayTime = x._1.dayTime
      val host = x._1.host
      val fileName = (SysConfig.URL_STAGE_NEW, host, UUID.randomUUID()).productIterator.mkString("#") + ".txt"
      val fileNamePath = SysConfig.URL_TEMP_DIR + dayTime + "/" + fileName
      val stream = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(fileNamePath))
      var writer = new PrintWriter(stream)
      var writerCount = 0
      x._2.grouped(1000).foreach(x => {
        writerCount += 1
        if (writerCount < 100) {
          writer.write(x.mkString("\n") + "\n")
        } else {
          writer.close()
          writerCount=0
          val newFileName = (SysConfig.URL_STAGE_NEW,host,UUID.randomUUID()).productIterator.mkString("#") + ".txt"
          val newFileNamePath = SysConfig.URL_TEMP_DIR + dayTime + "/" + newFileName
          val newStream = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(newFileNamePath))
          writer = new PrintWriter(newStream)
        }
      })
      writer.close()
    })
    cachedRdd.unpersist()
    sparkContext.parallelize(dealFileArray.toSeq).foreach(file => {
      val srcPath = new Path(file)
      FileSystemFactory.getObfFs(rawBucket).delete(srcPath, true)
    })
  }


  private def handleFilesOverSea(sparkContext: SparkContext,
                                 ipSrcBroadcast: Broadcast[Map[Int, Array[covertedIpSegment]]],
                                 count: Int,
                                 domainTableBroadcast: Broadcast[Map[String, DomainInfo]],
                                 rawBucket: String,
                                 vendor: String,
                                 dealFileArray: Array[String]): Unit = {
    //首先将文件放入待合并目录，不需要定制格式
    val rawRdd = sparkContext.textFile(dealFileArray.mkString(","))
    val cachedRdd = rawRdd.map(line => {
      val logRegex: Regex = """\[(.+?)\] (.+?) (.+?) "(.+?)" "(.+?)" "(.+?)" "(.+?)" "(.+?)" (.+?) (.+?) (.+?) "(.+?)"""".r
      val tencentLog: TencentLog = logRegex.findFirstIn(line) match {
        case Some(logRegex(dateTime, ip, responseTime, referer, protocol, method, host, url, httpCode, size, cacheState, userAgent)) =>
          TencentLog(dateTime, ip, responseTime, referer, protocol, method, host, url.split("\\?")(0), httpCode, size, cacheState, userAgent)
        case _ => LOGGER.error("parse tencent log failed, line " + line)
          null
      }
      if (tencentLog != null) {
        val provinceOperator = getProvinceOperator(tencentLog.ip, ipSrcBroadcast)
        val requestTime = getRequestTime(tencentLog.dateTime)
        val fiveMinPeriod = requestTime.getTime / 300000
        getHourTime(requestTime)
        (fiveMinuteKey(tencentLog.host, getHourTime(requestTime), fiveMinPeriod), statUnit(tencentLog.uri, provinceOperator.province, provinceOperator.operator, tencentLog.size.toLong))
      } else {
        null
      }
    }).filter(x => x != null).groupByKey(dealFileArray.length).map(x => {
      val statUnits = x._2.toArray
      val key = x._1
      //输出基于域名5分钟的URL临时统计值
      val uriStat = statUnits.map(statUnit => (statUnit.uri, statUnit.traffic)).groupBy(x => x._1).map(x => uriStatUnit(key.host, key.host + x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
      //输出基于域名输出5分钟的省份临时统计值
      val host = key.host
      val domainInfo = domainTableBroadcast.value
      domainInfo.get(host) match {
        case Some(a) => (FiveMinStatKey(host, a.userId, a.domainId, key.hourTime, key.fiveMinPeriod), uriStat)
        case _ =>
          val extensiveDomain = host.replace(host.split("\\.")(0), "*")
          domainInfo.get(extensiveDomain) match {
            case Some(a) => (FiveMinStatKey(extensiveDomain, a.userId, a.domainId, key.hourTime, key.fiveMinPeriod), uriStat)
            case _ =>
              LOGGER.warn("domain is not match, will be filted. " + host)
              (null, null)
          }
      }
    }).filter(x => x._1 != null).persist(StorageLevel.DISK_ONLY)

    val tempUriRdd = cachedRdd.map(x => {
      val dayTime = x._1.hourTime.substring(0, 8)
      val host = x._1.host
      x._2.map(x => ((dayTime, host, x.uri), (x.count, x.traffic)))
    }).flatMap(x => x).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2)).map(x => {
      val dayTime = x._1._1
      val host = x._1._2
      val uri = x._1._3
      val count = x._2._1
      val traffic = x._2._2
      (uriKey(dayTime, host), (host, uri, count, traffic).productIterator.mkString(SysConfig.URLSPLIT))
    })

    //按照域名和天groupby
    val writerRdd = tempUriRdd.groupByKey()
    writerRdd.foreach(x => {
      val dayTime = x._1.dayTime
      val host = x._1.host
      val fileName = (SysConfig.URL_STAGE_NEW, host, UUID.randomUUID()).productIterator.mkString("#") + ".txt"
      val fileNamePath = SysConfig.URL_TEMP_DIR_OVERSEA + dayTime + "/" + fileName
      val stream = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(fileNamePath))
      val writer = new PrintWriter(stream)
      x._2.grouped(1000).foreach(x => {
        writer.write(x.mkString("\n") + "\n")
      })
      writer.close()
    })
    cachedRdd.unpersist()
    sparkContext.parallelize(dealFileArray.toSeq).foreach(file => {
      val srcPath = new Path(file)
      FileSystemFactory.getObfFs(rawBucket).delete(srcPath, true)
    })
  }

  private def getRequestTime(time: String): Date = {
    val format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
    format.parse(time)

  }
}