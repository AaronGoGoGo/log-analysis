package log.analysis.preprocess.util

import java.sql.{Connection, DriverManager, SQLException, Statement}

import DataProcessVo
import log.analysis.preprocess.FiveMinuteJob.{LOGGER, TableIspKey}
import java.util.{Calendar, Date, UUID}

import log.analysis.common.{FileSystemFactory, SysConfig}
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

object DbWriter {
  def writeByRest(dataProcessVo : DataProcessVo): Unit = {
    //    println(s"+++++++ result ${JsonUtil.convertBean2Json(dataProcessVo)}")
    //    RestClient.getInstance().put("http://10.62.32.172:8082/v1.0/cdn/statistic-background-jobs/process-kafka-data", JsonUtil.convertBean2Json(dataProcessVo),getRequestHeaders)
  }

  def writeIsp(sqls : Array[String]): Unit =
  {
    try {
      var con: Connection = null
      var statement: Statement = null
      try { //postgresql驱动名称
        Class.forName("org.postgresql.Driver")
        //数据库连接路径
        val url = SysConfig.JDBC_ISP_ENDPOINT
        //            DriverManager.getConnection(url, "GaussDB", "gaussdb@123")
        con = DriverManager.getConnection(url, SysConfig.JDBC_ISP_USER, SysConfig.JDBC_ISP_PASSWORD)
        con.setAutoCommit(false)
      } catch {
        case e: Exception =>
          LOGGER.error("getConnection fail ", e)
          throw new RuntimeException(e.getMessage, e)
      }
      try {
        statement = con.createStatement
        //            val sqlString = "SELECT UPSERT_STATISTIC_ITEM_SUM_FLUX_MINUTE5_1(999,999,3,-1,-1,0,0,0,58741592,0,0,0,0,0,0,0,0,58741592);"
        //            statement.execute(sqlString)
        sqls.foreach(sql => {
          //        LOGGER.error(sql)
          //                preparedStatement = con.prepareStatement(sql)
          statement.execute(sql)
        })
      }
      catch {
        case e: SQLException => con.rollback()
          //        LOGGER.error("rollback", e)
          throw new RuntimeException("rollback", e)
      }
      finally {
        if (null != con) con.commit()
        if (null != statement) statement.close()
      }
    } catch {
      case ex : Exception => {
        LOGGER.error("handle sql fail.", ex)
        val sqlPath = "/ISP_FAILED_SQL/" + new Date().getTime + "-" + UUID.randomUUID() + ".sql"
        val out = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(sqlPath))
        out.writeBytes(sqls.mkString("\n"))
        out.close()
      }
    }
  }

  def generateMinute5Sqls(userId : Int, minute5All : Array[(TableIspKey, ((Int, Long),Long))],vendor : String): Array[String] =
  {
    val calendar = Calendar.getInstance
    val result = minute5All.groupBy(x => x._1).map(x => {
      var trafficValues = new Array[Long](13)
      var countValues = new Array[Int](13)
      var sqlArray = new ArrayBuffer[String]()

      x._2.map(x => x._2).foreach(record => {
        val fiveMinPeriod = record._2
        calendar.setTimeInMillis(fiveMinPeriod * 300000)
        val minute = calendar.get(Calendar.MINUTE)
        val valueInx = minute / 5
        val traffic = record._1._2
        val count = record._1._1
        trafficValues(valueInx) += traffic
        trafficValues(12) += traffic
        countValues(valueInx) += count
        countValues(12) += count
      })
      val keyString = x._1.productIterator.mkString(",")
      val fluxValueString = trafficValues.mkString(",")
      val reqNumValueString = countValues.mkString(",")
      val sumFluxSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_FLUX_MINUTE5_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val sumReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_REQ_NUM_MINUTE5_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"
      val uCDNFluxSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_FLUX_MINUTE5_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val uCDNReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_REQ_NUM_MINUTE5_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"

      sqlArray += sumFluxSql += sumReqNumSql += uCDNFluxSql += uCDNReqNumSql
      sqlArray.toArray
    })
    result.toArray.flatten
  }

  def generateHour4Sqls(userId : Int, dayAll : Array[(TableIspKey, ((Int, Long),Long))],vendor : String): Array[String] =
  {
    val calendar = Calendar.getInstance
    val result = dayAll.groupBy(x => x._1).map(x => {
      var trafficValues = new Array[Long](7)
      var countValues = new Array[Int](7)
      var sqlArray = new ArrayBuffer[String]()

      x._2.map(x => x._2).foreach(record => {
        val fiveMinPeriod = record._2
        calendar.setTimeInMillis(fiveMinPeriod * 300000)
        val hour = calendar.get(Calendar.HOUR_OF_DAY)
        val valueInx = hour / 4
        val traffic = record._1._2
        val count = record._1._1
        trafficValues(valueInx) += traffic
        trafficValues(6) += traffic
        countValues(valueInx) += count
        countValues(6) += count
      })
      val keyString = x._1.productIterator.mkString(",")
      val fluxValueString = trafficValues.mkString(",")
      val reqNumValueString = countValues.mkString(",")
      val sumFluxSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_FLUX_HOUR4_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val sumReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_REQ_NUM_HOUR4_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"
      val uCDNFluxSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_FLUX_HOUR4_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val uCDNReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_REQ_NUM_HOUR4_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"

      sqlArray += sumFluxSql += sumReqNumSql += uCDNFluxSql += uCDNReqNumSql
      sqlArray.toArray
    })
    result.toArray.flatten
  }

  def generateHour8Sqls(userId : Int, dayAll : Array[(TableIspKey, ((Int, Long),Long))],vendor : String): Array[String] =
  {
    val calendar = Calendar.getInstance
    val result = dayAll.groupBy(x => x._1).map(x => {
      var trafficValues = new Array[Long](4)
      var countValues = new Array[Int](4)
      var sqlArray = new ArrayBuffer[String]()

      x._2.map(x => x._2).foreach(record => {
        val fiveMinPeriod = record._2
        calendar.setTimeInMillis(fiveMinPeriod * 300000)
        val hour = calendar.get(Calendar.HOUR_OF_DAY)
        val valueInx = hour / 8
        val traffic = record._1._2
        val count = record._1._1
        trafficValues(valueInx) += traffic
        trafficValues(3) += traffic
        countValues(valueInx) += count
        countValues(3) += count
      })
      val keyString = x._1.productIterator.mkString(",")
      val fluxValueString = trafficValues.mkString(",")
      val reqNumValueString = countValues.mkString(",")
      val sumFluxSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_FLUX_HOUR8_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val sumReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_REQ_NUM_HOUR8_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"
      val uCDNFluxSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_FLUX_HOUR8_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val uCDNReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_REQ_NUM_HOUR8_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"

      sqlArray += sumFluxSql += sumReqNumSql += uCDNFluxSql += uCDNReqNumSql
      sqlArray.toArray
    })
    result.toArray.flatten
  }

  def generateDay1Sqls(userId : Int, dayAll : Array[(TableIspKey, ((Int, Long),Long))], vendor : String): Array[String] =
  {
    val calendar = Calendar.getInstance
    val result = dayAll.groupBy(x => x._1).map(x => {
      var trafficValues = new Array[Long](1)
      var countValues = new Array[Int](1)
      var sqlArray = new ArrayBuffer[String]()

      x._2.map(x => x._2).foreach(record => {
        val fiveMinPeriod = record._2
        calendar.setTimeInMillis(fiveMinPeriod * 300000)
        val hour = calendar.get(Calendar.HOUR_OF_DAY)
        val traffic = record._1._2
        val count = record._1._1
        trafficValues(0) += traffic
        countValues(0) += count
      })
      val keyString = x._1.productIterator.mkString(",")
      val fluxValueString = trafficValues.mkString(",")
      val reqNumValueString = countValues.mkString(",")
      val sumFluxSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_FLUX_DAY1_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val sumReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_SUM_REQ_NUM_DAY1_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"
      val uCDNFluxSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_FLUX_DAY1_" + userId % 20 + "(" + keyString + "," + fluxValueString + ");"
      val uCDNReqNumSql = "SELECT UPSERT_STATISTIC_ITEM_" + vendor + "_REQ_NUM_DAY1_" + userId % 20 + "(" + keyString + "," + reqNumValueString + ");"

      sqlArray += sumFluxSql += sumReqNumSql += uCDNFluxSql += uCDNReqNumSql
      sqlArray.toArray
    })
    result.toArray.flatten
  }

}