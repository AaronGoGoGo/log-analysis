package log.analysis.preprocess

import java.util.Calendar

import log.analysis.common.SysConfig
import log.analysis.preprocess.FiveMinuteJob.{LOGGER, TableIspKey, ispAllLabel}
import log.analysis.preprocess.util.DbWriter
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object IspExcutor {
  def dealIsp(ispRdd: RDD[(FiveMinuteJob.FiveMinStatKey, Array[FiveMinuteJob.ProvinceAndOperatorStatUnit])],vendor : String) {
    val sqls = ispRdd.groupBy(x => x._1.userId).map(x => {
      val userId = x._1
      val minute5All = x._2.flatMap(x => {
        val fiveMinPeriod = x._1.fiveMinPeriod
        val hostId = x._1.hostId
        //通过5分钟中周期换算出小时周期
        val hourPoint = x._1.fiveMinPeriod / 12 * 3600000
        val operatorAll = x._2.map(x => {
          (TableIspKey(hostId, hourPoint, userId, x.item.operator, ispAllLabel), (x.count, x.traffic))
        }).groupBy(x => x._1).mapValues(x => x.map(x => x._2).reduce((x1, x2) => {
          (x1._1 + x2._1, x1._2 + x2._2)
        })).toArray
        val provinceAll = x._2.map(x => {
          (TableIspKey(hostId, hourPoint, userId, ispAllLabel, x.item.province), (x.count, x.traffic))
        }).groupBy(x => x._1).mapValues(x => x.map(x => x._2).reduce((x1, x2) => {
          (x1._1 + x2._1, x1._2 + x2._2)
        })).toArray
        x._2.map(x => {
          //5分钟表确认按照摩20分表
          (TableIspKey(hostId, hourPoint, userId, x.item.operator, x.item.province), (x.count, x.traffic))
        }).union(operatorAll).union(provinceAll).map(x => (x._1, (x._2, fiveMinPeriod)))
      }).toArray
      val dayAll = minute5All.map(x => {
        val calendar = Calendar.getInstance
        calendar.setTimeInMillis(x._1.hourPoint)

        calendar.set(Calendar.HOUR_OF_DAY, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val dayPoint = calendar.getTimeInMillis
        (TableIspKey(x._1.hostId, dayPoint, x._1.userId, x._1.operator, x._1.province), x._2)
      })

      val sqlArray = new ArrayBuffer[String]()
      //                    val tempuserId = new Random().nextInt(10000)
      vendor match {
        case SysConfig.UCDN | SysConfig.TENCENT | SysConfig.CHINANETCENTER =>
          sqlArray ++= DbWriter.generateMinute5Sqls(userId, minute5All, vendor)
          sqlArray ++= DbWriter.generateHour4Sqls(userId, dayAll, vendor)
          sqlArray ++= DbWriter.generateHour8Sqls(userId, dayAll, vendor)
          sqlArray ++= DbWriter.generateDay1Sqls(userId, dayAll, vendor)
      }
//      sqlArray.toArray.foreach(x => LOGGER.info("sql ------------ " + x))
      DbWriter.writeIsp(sqlArray.toArray)
      LOGGER.info("=============print sqls  + num : " + sqlArray.toArray.length)
      (userId,sqlArray.toArray.length)
    }).collect().foreach(x => {
      val userId = x._1
      val sqlNum = x._2
      LOGGER.info(s"=========== userId $userId ----- sql num is $sqlNum")
    })
//    sqls.foreach(x => LOGGER.info("sql ------------ " + x))
//    LOGGER.info("sql num is " + sqls.length)

  }

}
