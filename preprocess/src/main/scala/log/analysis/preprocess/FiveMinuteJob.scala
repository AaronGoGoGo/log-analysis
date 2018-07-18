package log.analysis.preprocess

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.zip.GZIPOutputStream
import java.util.{Date, Locale, UUID}

import log.analysis.common.CommonUtil.DomainInfo
import log.analysis.common.{CommonUtil, FileSystemFactory, SysConfig, WccUtil}
import log.analysis.preprocess.ThirdPartyExcutor.LOGGER
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Random


object FiveMinuteJob {

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[(String, String), Any] {
    override def generateFileNameForKeyValue(key: (String, String), value: Any, name: String): String = {
      key._1 + "#" + key._2 + "#" + UUID.randomUUID() + ".txt";
    }
  }

  case class XXComsumer(requestStartTime: String, clientIP: String, domain: String,
                            url: String, size: String, province: Int,
                            operator: Int, httpCode: String, referer: String, requestDurationTime: Long,
                            userAgent: String, range: String, method: String, httpProtocol: String,
                            cacheState: String, vip: String, disConnectReason: String)

  case class CommonLogFormat(requestStartTime : String,clientIP : String, requestDurationTime: Long, referer : String,
                             httpProtocol : String, method : String, domain : String, uri: String, httpCode : String, size : String,
                             cacheState : String, userAgent : String, range : String) {
    override def toString: String = s"""[$requestStartTime] $clientIP $requestDurationTime "$referer" "$httpProtocol" "$method" "$domain" "$uri" $httpCode $size $cacheState "$userAgent" "$range""""
  }

  case class RawLogFormat(vip: String, userIp: String, userRequestIp: String, method: String, httpVersion: String,
                          host: String, uri: String, userAgent: String, referer: String, contentType: String,
                          statusCode: String, cacheState: String, cacheOutPutTraffic: String, requestStartTime: String,
                          requestEndTime: String, range: String, disConnectReason: String, province: Int, operator: Int)

  case class domainUriPair(domain: String, uri: String)

  case class provinceAndOperator(province: Int, operator: Int)

  case class covertedIpSegment(startIp: Int, endIp: Int, country: String, province: Int, operator: Int)

  case class statUnit(uri: String, province: Int, operator: Int, traffic: Long)

  case class uriStatUnit(host: String, uri: String, count: Int, traffic: Long)

  case class ProvinceAndOperatorStatUnit(item: provinceAndOperator, count: Int, traffic: Long)

  case class provinceStatUnit(province: Int, count: Int, traffic: Long)

  case class operatorStatUnit(operator: Int, count: Int, traffic: Long)

  case class fiveMinuteKey(host: String, hourTime: String, fiveMinPeriod: Long)

  case class uriKey(dayTime: String, host: String)

  case class FiveMinStatKey(host: String, userId: Int, hostId: Int, hourTime: String, fiveMinPeriod: Long)

  case class Minute5Table(hostId: Int, hourPoint: Long, userId: Int, operator: Int, province: Int,
                          value0: Long, value1: Long, value2: Long, value3: Long, value4: Long, value5: Long,
                          value6: Long, value7: Long, value8: Long, value9: Long, value10: Long, value11: Long, sum: Long)

  case class TableIspKey(hostId: Int, hourPoint: Long, userId: Int, operator: Int, province: Int)

  val LOGGER = LoggerFactory.getLogger(FiveMinuteJob.getClass)

  val ispAllLabel = -999

  val provinceMap = Map("北京" -> 11, "天津" -> 12, "河北" -> 13, "山西" -> 14, "内蒙古" -> 15,
    "辽宁" -> 21, "吉林" -> 22, "黑龙江" -> 23,
    "上海" -> 31, "江苏" -> 32, "浙江" -> 33, "安徽" -> 34, "福建" -> 35, "江西" -> 36, "山东" -> 37,
    "河南" -> 41, "湖北" -> 42, "湖南" -> 43, "广东" -> 44, "广西" -> 45, "海南" -> 46,
    "重庆" -> 50, "四川" -> 51, "贵州" -> 52, "云南" -> 53, "西藏" -> 54,
    "陕西" -> 61, "甘肃" -> 62, "青海" -> 63, "宁夏" -> 64, "新疆" -> 65,
    "台湾" -> 1, "香港" -> 1, "澳门" -> 1)
  val operatorMap = Map("电信" -> 1, "联通" -> 2, "移动" -> 3, "铁通" -> 4, "教育网" -> 5, "鹏博士" -> 6)

  val partitionSize: Int = SysConfig.JOB_5MIN_PARTITION_SIZE

  def main(args: Array[String]): Unit = {

    if (args == null || args.length != 1) {
      println("wrong input! writelist")
      return
    }
    val whitelist = args(0).split("#")

    val ipSourceIn = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).open(new Path("/mydata4vipday2.txt"))
    val file = Source.fromInputStream(ipSourceIn, "UTF-8")
    val ipSource = file.getLines().toArray.filter(line => line.split("\t")(2) == "中国").map(line => {
      /**
        * IP 库字段顺序 起始IP 结束IP 国家 省 地区 公司 运营商 未知
        */
      val fields = line.split("\t")
      covertedIpSegment(ipToInt(fields(0)), ipToInt(fields(1)), fields(2), getProvinceCode(fields(3)), getOperatorCode(fields(6)))
    }).groupBy(x => x.startIp >> 16)


    val conf = new SparkConf
    conf.set("spark.hadoop.fs.s3a.endpoint", SysConfig.OBS_ENDPOINT)
    conf.set("spark.hadoop.fs.s3a.access.key", SysConfig.OBS_AK)
    conf.set("spark.hadoop.fs.s3a.secret.key", WccUtil.getInstance().decrypt(SysConfig.OBS_SK))
    conf.setAppName("CDN_5Min_Job")
    //    val ss = SparkSession.builder().appName("CDN_5Min_Job").config(conf).getOrCreate()
    //    val sparkContext: SparkContext = new SparkContext(conf)
    val sparkContext: SparkContext = new SparkContext(conf)
    val ipSrcBroadcast = sparkContext.broadcast(ipSource)


    var count = 1000
    val processStart = new Date().getTime
    LOGGER.info("start 5Min Job")
    while (count > 0) {
      if (new Date().getTime - processStart > 24 * 3600 * 1000) {
        LOGGER.info("reach specified limit time.")
        sparkContext.stop()
        return
      }

      if (FileSystemFactory.getHdfsFs.exists(new Path("/cdn/config/5MinStop.md"))) {
        LOGGER.info("find stop label.")
        FileSystemFactory.getHdfsFs.delete(new Path("/cdn/config/5MinStop.md"), true)
        sparkContext.stop()
        return
      }

      val domainTableBroadcast = CommonUtil.genDomainInfoBroadCast(sparkContext)

      try {

        //需要增加调度能力
        dealUcdnRawLog(sparkContext, ipSrcBroadcast, count, domainTableBroadcast,whitelist)
        ThirdPartyExcutor.dealRawLog(sparkContext, ipSrcBroadcast, count, domainTableBroadcast,SysConfig.TENCENT_RAW_DATA_BUCKET,SysConfig.TENCENT, isOverSea = false)
        ThirdPartyExcutor.dealRawLog(sparkContext, ipSrcBroadcast, count, domainTableBroadcast,SysConfig.CNC_RAW_DATA_BUCKET,SysConfig.CHINANETCENTER, isOverSea = false)

        ThirdPartyExcutor.dealRawLog(sparkContext, ipSrcBroadcast, count, domainTableBroadcast,SysConfig.TENCENT_OVERSEA_RAW_DATA_BUCKET,SysConfig.TENCENT, isOverSea = true)
        ThirdPartyExcutor.dealRawLog(sparkContext, ipSrcBroadcast, count, domainTableBroadcast,SysConfig.CNC_OVERSEA_RAW_DATA_BUCKET,SysConfig.CHINANETCENTER, isOverSea = true)
      }
      catch {
        case ex: Throwable => LOGGER.error("test exception", ex)
          sparkContext.stop()
          return
      }
      Thread.sleep(10000)
    } //while 循环结束
    sparkContext.stop()
  }


  private def dealUcdnRawLog(sparkContext: SparkContext, ipSrcBroadcast: Broadcast[Map[Int, Array[covertedIpSegment]]], count: Int, domainTableBroadcast: Broadcast[Map[String, DomainInfo]],whitelist : Array[String]): Unit = {
    val obsRawFs = FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET)
    if (!obsRawFs.exists(new Path(SysConfig.RAW_DATA_DIR))) {
      obsRawFs.mkdirs(new Path(SysConfig.RAW_DATA_DIR))
    }
    var totalSize = 0L
    val dealFileArray = obsRawFs.listStatus(new Path(SysConfig.RAW_DATA_DIR))
      .filter(fileStatus => fileStatus.isFile && fileStatus.getPath.getName.matches(".*\\.log.*"))
      .sortBy(fileStatus => fileStatus.getModificationTime)
      .takeWhile(fileStatus => {
        totalSize += fileStatus.getLen

        totalSize < SysConfig.JOB_5MIN_INPUT_SIZE * 1024 * 1024
      })
      .map(fileStatus => fileStatus.getPath.getParent + "/" + fileStatus.getPath.getName)
    try {
      if (dealFileArray.length != 0) {
        val dealString: String = dealFileArray.mkString(",")
        LOGGER.info("dealString  is " + dealString)
        LOGGER.info("dealFileArray size is " + dealFileArray.length)
        LOGGER.info("deal ucdn raw log")
        LOGGER.info("----totalSize " + totalSize)
        val start = new Date().getTime
        val rawRdd = sparkContext.textFile(dealString)
        val visitTimes = rawRdd.map(line => {
          val groupInfo = getHostAndStartTimeFromRawLog(line)
          val startDate = getTime(groupInfo._2)
          val host = groupInfo._1
          (fiveMinuteKey(host, getHourTime(startDate), startDate.getTime / 300000), 1)
        }).groupByKey().map(x => (x._1, x._2.size)).collectAsMap()
        //
        val partitionNum = visitTimes.values.sum / partitionSize + 1
        val groupByPartition = visitTimes.map(x => x._2 / partitionSize + 1).sum
        visitTimes.foreach(x => LOGGER.info("(" + x._1 + " , " + x._2 + ")"))
        val customizedRdd = rawRdd.repartition(partitionNum).map(line => {
          val groupInfo = getHostAndStartTimeFromRawLog(line)
          val startDate = getTime(groupInfo._2)
          val r = new Random()
          val hourTime = getHourTime(startDate)
          val host = groupInfo._1
          val fiveMinPeriod = startDate.getTime / 300000
          val key = fiveMinuteKey(host, hourTime, fiveMinPeriod)
          val size =
            try { visitTimes(key)}
          catch {
            case ex : Exception => throw new Exception(key.toString)
          }
          val rawLog = extractRawLog(line, ipSrcBroadcast)
          val outputLine =  if (whitelist.contains(host)) {
            val xxComsumer = XXComsumer(getHuaweiConsumerTime(getTime(rawLog.requestStartTime)), rawLog.userIp, rawLog.host,
              rawLog.uri, rawLog.cacheOutPutTraffic, rawLog.province, rawLog.operator,
              rawLog.statusCode, rawLog.referer, getTime(rawLog.requestEndTime).getTime - getTime(rawLog.requestStartTime).getTime, rawLog.userAgent, rawLog.range, rawLog.method,
              rawLog.httpVersion, rawLog.cacheState, rawLog.vip, rawLog.disConnectReason)
            xxComsumer.productIterator.mkString("||")
          } else {
            val referer = if (rawLog.referer.equals("NULL"))
              {
                "-"
              } else {
              rawLog.referer
            }

            val range = if (rawLog.range.equals("NULL"))
            {
              "-"
            } else {
              rawLog.range
            }
            val commonLogFormat = CommonLogFormat(getCommonLogTime(getTime(rawLog.requestStartTime)),rawLog.userIp,getTime(rawLog.requestEndTime).getTime - getTime(rawLog.requestStartTime).getTime,
              referer,rawLog.httpVersion,rawLog.method,rawLog.host,rawLog.uri,rawLog.statusCode,rawLog.cacheOutPutTraffic,rawLog.cacheState,
              rawLog.userAgent,range)
            commonLogFormat.toString
          }

          val cache = try {
            rawLog.cacheOutPutTraffic.toLong
          } catch {
            case e : Exception => 0L
          }
          /*getTime(rawLog.requesoutputLinetStartTime).getTime + "||" + */
          ((key, r.nextInt(size / partitionSize + 1)), (outputLine, statUnit(rawLog.uri, rawLog.province, rawLog.operator, cache)))
        })
        val cachedStatRdd = customizedRdd.groupByKey(groupByPartition).map(x => {
          val key = x._1._1
          val fileName = key.host + "#" + key.hourTime + "#" + key.fiveMinPeriod + "#" + UUID.randomUUID() + ".gz"
          val fileNamePath = SysConfig.MERGED_DIR + fileName
          val stream = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(fileNamePath))
          val gzipOut = new GZIPOutputStream(stream)
          val writer = new PrintWriter(gzipOut)

          val xx = new Date().getTime
          x._2.map(x => x._1).grouped(1000).foreach(x => {
            writer.write(x.mkString("\n") + "\n")
          })
          writer.close()
          val statUnits = x._2.map(x => x._2).toArray
          //输出基于域名5分钟的URL临时统计值
          val uriStat = statUnits.map(statUnit => (statUnit.uri, statUnit.traffic)).groupBy(x => x._1).map(x => uriStatUnit(key.host, key.host + x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
          //输出基于域名输出5分钟的省份和运营商的临时统计值
          val poStat = statUnits.map(statUnit => (provinceAndOperator(statUnit.province, statUnit.operator), statUnit.traffic)).groupBy(x => x._1).map(x => ProvinceAndOperatorStatUnit(x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
          //输出基于域名输出5分钟的运营商临时统计值
          //          val operatorStat = statUnits.map(statUnit => (statUnit.operator, statUnit.traffic)).groupBy(x => x._1).map(x => operatorStatUnit(x._1, x._2.length, x._2.map(x => x._2).sum)).toArray
//          System.out.println("hhhhhhh write file takes " + (new Date().getTime - xx) + ", size " + x._2.size)
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
          //输出基于域名5分钟的URL临时统计值
          //            rawLogs.map(rawLog => rawLog.uri).groupBy(x=>x).map(x=>(x._1,x._2.size))
          //输出基于域名输出5分钟的省份临时统计值
          //            rawLogs.map(rawLog => rawLog.province).groupBy(x=>x).map(x=>(x._1,x._2.size))
          //输出基于域名输出5分钟的运营商临时统计值
          //            rawLogs.map(rawLog => rawLog.operator).groupBy(x=>x).map(x=>(x._1,x._2.size))
        }).filter(x => x._1 != null).cache()
        //省份和运营商按照用户进行分库分表，一个用户只建立一次JDBC连接,按照userId进行GroupBy
        IspExcutor.dealIsp(cachedStatRdd.map(x => (x._1, x._2._2)), SysConfig.UCDN)
        val tempUriRdd = cachedStatRdd.groupByKey().flatMap(x => {
          val dayTime = x._1.hourTime.substring(0, 8)
          val host = x._1.host
          x._2.flatMap(x => x._1.map(x => ((dayTime, host, x.uri), (x.count, x.traffic))))
        }).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2)).map(x => {
          val dayTime = x._1._1
          val host = x._1._2
          (uriKey(dayTime, host), (host, x._1._3, x._2._1, x._2._2).productIterator.mkString(SysConfig.URLSPLIT))
        })

        //按照域名和天groupby
        val writerRdd = tempUriRdd.groupByKey()
        writerRdd.foreach(x => {
          val dayTime = x._1.dayTime
          val host = x._1.host
          val fileName = (SysConfig.URL_STAGE_NEW,host,UUID.randomUUID()).productIterator.mkString("#") + ".txt"
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

        val format = new SimpleDateFormat("yyyyMMddHH")
        val timeString = format.format(new Date())
        val day = timeString.substring(0, 8)
        val hour = timeString.substring(8)
        val destBaseDir = SysConfig.RAW_DATA_DONE_DIR + day + "/" + hour + "/"
        if (!FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).exists(new Path(destBaseDir))) {
          FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).mkdirs(new Path(destBaseDir))
        }
        sparkContext.parallelize(dealFileArray.toSeq).foreach(file => {
          val srcPath = new Path(file)
          val destPath = new Path(destBaseDir + srcPath.getName)
          if (!FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).rename(srcPath, destPath)) {
            FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).delete(srcPath, true)
          }
        })

        cachedStatRdd.unpersist()

        LOGGER.info("---------------- job take time " + (new Date().getTime - start))

      }

    }
    catch {
      case ex: Throwable => LOGGER.error("test exception", ex)
        if (!FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).exists(new Path(SysConfig.RAW_DATA_FAIL_DIR))) {
          FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).mkdirs(new Path(SysConfig.RAW_DATA_FAIL_DIR))
        }
        sparkContext.parallelize(dealFileArray.toSeq).foreach(file => {
          val srcPath = new Path(file)
          val destPath = new Path(SysConfig.RAW_DATA_FAIL_DIR + srcPath.getName)
          if (!FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).rename(srcPath, destPath)) {
            FileSystemFactory.getObfFs(SysConfig.RAW_DATA_BUCKET).delete(srcPath, true)
          }
        })
        throw new Exception(ex)
    } //while 循环结束
  }


  def getFailArray(failArray : Array[String]): Array[String] =
  {
    val userIp = "0.0.0.0"
    val requestIp = "0.0.0.0"
    val domain = "www.faillabel.com"
    val uri = "http://" + domain + failArray.mkString("###")
    val cacheState = "MISS"
    val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")

    val requestStartTime = format.format(new Date(0))
    val requestEndTime = requestStartTime
    Array("NULL", "0.0.0.0", userIp, requestIp, "GET",
      "HTTP/1.1", domain, uri, "\"DoubanMovie/3.6.3 CFNetwork/711.0.6 Darwin/14.0.0\"", domain,
      "application/octet-stream", "200", cacheState, "9500", "1048957",
      requestStartTime, requestEndTime, "NULL","hash", "type",
      "hostIp", "hostdstIp", "NULL", "NULL", "NULL",
      "NULL", "NULL", "200", "0", "max-age=1800",
      "NULL", "1048576", "0", "6388", "1048957",
      "NULL", "NULL", "NULL","NULL", "NULL",
      "woshirange","0")
  }

  def extractRawLog(line: String, ipSrcBroadcast: Broadcast[Map[Int, Array[covertedIpSegment]]]): RawLogFormat = {
    var array = line.split("\\|")
    //uCDN原始数据
    if (array.length != 43 && array.length != 42) {
      LOGGER.error("raw file length is wrong, " + line)
      array = getFailArray(array)
    }
    //内网得FI版本太低，不支持此种写法
    //    val Array(
    //      _,requestStartTime,clientIP,domain,url,
    //    size,province,operator,httpCode,referer,
    //    requestDurationTime,userAgent,range,_,method,
    //    httpProtocol,cacheState,_,_,_,
    //    _,_,_,_,_,
    //    _,_,_,_,_,
    //    _,_,_,_,_,
    //    _,_,_,_,vip,
    //    disConnectReason
    //    ) = array

    var vip = array(1)
    var userIp = array(2)
    var userRequestIp = array(3)
    var method = array(4)
    var httpVersion = array(5)
    var host = array(6)
    var url = array(7)
    var userAgent = array(8)
    var referer = array(9)
    var contentType = array(10)
    var statusCode = array(11)
    var cacheState = array(12)
    var cacheOutPutTraffic = array(14)
    var requestStartTime = array(15)
    var requestEndTime = array(16)
    var range = array(40)
    var disConnectReason = array(41)

    //uri取？号之后
    var uri = if (url.indexOf(host) == -1) {
      "/"
    } else {
      url.substring(url.indexOf(host) + host.length)
    }.split("\\?")(0)

    val provinceOperator =
     try {
       getProvinceOperator(userIp, ipSrcBroadcast)
     }catch
       {
         case e : Throwable =>
           LOGGER.error("line " + line, e)
           throw new Exception("line " + line, e)
       }
    val rawLog = RawLogFormat(vip, userIp, userRequestIp, method,httpVersion,
      host, uri, userAgent, referer, contentType,
      statusCode, cacheState, cacheOutPutTraffic, requestStartTime,
      requestEndTime, range, disConnectReason, provinceOperator.province, provinceOperator.operator)
    rawLog
  }

  def getHostAndStartTimeFromRawLog(line: String): (String, String) = {
    var array = line.split("\\|")
    //uCDN原始数据
    if (array.length != 43 && array.length != 42) {
      LOGGER.error("raw file length is wrong, " + line)
      array = getFailArray(array)
    }
    (array(6), array(15))
  }

  def getTime(time: String): Date = {
    try {
      val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
      format.parse(time)
    } catch {
      case e : Exception  =>
        LOGGER.error("getTime error", e)
        new Date(0)
    }

  }

  def getHourTime(date: Date): String = {
    val format = new SimpleDateFormat("yyyyMMddHH")
    format.format(date)
  }

  def getHuaweiConsumerTime(date: Date): String = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    format.format(date)
  }

  def getCommonLogTime(date: Date): String = {
    val format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
    format.format(date)
  }

  def getProvinceCode(province: String): Int = {
    provinceMap.get(province) match {
      case Some(a) => a
      case _ => 0
    }
  }

  def getOperatorCode(operator: String): Int = {
    operatorMap.get(operator) match {
      case Some(a) => a
      case _ => 0
    }
  }

  def getProvinceOperator(source: String, ipSrcBroadcast: Broadcast[Map[Int, Array[covertedIpSegment]]]): provinceAndOperator = {
    try {
      val sourceInt = ipToInt(source)
      val matchArray = ipSrcBroadcast.value.get(sourceInt >> 16)
      matchArray match {
        case Some(a) => a.foreach(x => {
          if (x.startIp <= sourceInt && sourceInt <= x.endIp) {
            return provinceAndOperator(x.province, x.operator)
          }
        })
        case _ =>
          //不存在表示海外
          return provinceAndOperator(-1, -1)
      }
    }
    catch {
      case e: Exception =>
        LOGGER.error("getProvinceOperator fail, ip is " + source, e)
        return provinceAndOperator(0, 0)
    }
    return provinceAndOperator(0, 0)
  }

  def getDomainUri(url: String): domainUriPair = {
    val index1 = url.indexOf("//")
    val urlIndex = url.indexOf("/", index1 + 2)
    if (urlIndex == -1) {
      val uri = "/"
      val domain = url.substring(index1 + 2).split(":")(0)
      domainUriPair(domain, uri)
    }
    else {
      val domain = url.substring(index1 + 2).split(":")(0)
      val uri = url.substring(urlIndex)
      domainUriPair(domain, uri)
    }
  }

  def getXXComsumerStartTime(source: Date): String = {
    source.toString
  }

  def ipToInt(ip: String): Int = {
    val ips = ip.split("\\.")
    val ip0 = ips(0).toInt << 24
    val ip1 = ips(1).toInt << 16
    val ip2 = ips(2).toInt << 8
    val ip3 = ips(3).toInt
    ip0 | ip1 | ip2 | ip3
  }
}