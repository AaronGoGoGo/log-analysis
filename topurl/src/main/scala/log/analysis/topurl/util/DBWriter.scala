package log.analysis.topurl.util

import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.{Date, UUID}

import log.analysis.common.{FileSystemFactory, SysConfig}
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

object DBWriter {
  private val LOGGER = LoggerFactory.getLogger(DBWriter.getClass)

  def writeTopAccessUrl(userId: Int, hostId: Int, point: Long, urls: Array[(String, Long)],isOverSea : Boolean): Unit = {
    val tableName = if (isOverSea) "TOP_URL_ACCESS_NUM_OVERSEA" else "TOP_URL_ACCESS_NUM"
    try {
      writeTopUrl(userId, hostId, point, urls, tableName)
    }catch {
      case ex : Exception =>
        {
          LOGGER.error("writeTopUrl error ", ex)
          val sqlPath = "/TOP_ACCESS_URL_FAILED_SQL/" + new Date().getTime + "-" + UUID.randomUUID() + ".sql"
          val out = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(sqlPath))
          out.writeBytes(urls.map(url => (userId,hostId,point,url).productIterator.mkString(" ")).mkString("\n"))
          out.close()
        }
    }
  }

  def writeTopFluxUrl(userId: Int, hostId: Int, point: Long, urls: Array[(String, Long)], isOverSea : Boolean): Unit = {
    val tableName = if (isOverSea) "TOP_URL_FLUX_OVERSEA" else "TOP_URL_FLUX"
    try {
    writeTopUrl(userId,hostId,point,urls,tableName)
    }catch {
      case ex : Exception =>
        {
          LOGGER.error("writeTopUrl error ", ex)
          val sqlPath = "/TOP_ACCESS_FLUX_FAILED_SQL/" + new Date().getTime + "-" + UUID.randomUUID() + ".sql"
          val out = FileSystemFactory.getObfFs(SysConfig.MIDDLE_DATA_BUCKET).create(new Path(sqlPath))
          out.writeBytes(urls.map(url => (userId,hostId,point,url).productIterator.mkString(" ")).mkString("\n"))
          out.close()
        }
  }
  }


  private def writeTopUrl(userId: Int, hostId: Int, point: Long, urls: Array[(String, Long)], tableName : String): Unit = {
    var con: Connection = null
    try { //postgresql驱动名称
      Class.forName("org.postgresql.Driver")
      //数据库连接路径
      val url = SysConfig.JDBC_URL_ENDPOINT
      //            DriverManager.getConnection(url, "GaussDB", "gaussdb@123")
      con = DriverManager.getConnection(url, SysConfig.JDBC_URL_USER, SysConfig.JDBC_URL_PASSWORD)
      con.setAutoCommit(false)
    } catch {
      case e: Exception =>
        LOGGER.error("connection", e)
        throw new RuntimeException(e.getMessage, e)
    }
    try {
//      statement = con.createStatement
      val deleteSql = "delete from " + tableName + " where DOMAIN_NAME_ID = " + hostId + " and USER_DOMAIN_ID = " + userId + " and POINT = " + point + ";";
      //           val sqlString = "SELECT UPSERT_STATISTIC_ITEM_SUM_FLUX_MINUTE5_1(999,999,3,-1,-1,0,0,0,58741592,0,0,0,0,0,0,0,0,58741592);"
      LOGGER.error(deleteSql)
      val delPstmt = con.prepareStatement(deleteSql)
      delPstmt.execute()
      urls.foreach(x => {
        val url = x._1
        val count = x._2

        val insertSql = "INSERT INTO " + tableName + "(DOMAIN_NAME_ID,POINT,USER_DOMAIN_ID,VALUE,URL) VALUES (" +(hostId, point, userId, count, "?").productIterator.mkString(",") + ");";
        val pstmt = con.prepareStatement(insertSql)
        pstmt.setString(1,url)
        LOGGER.error(insertSql)
        pstmt.execute()
//        statement.execute(insertSql)
      })
    }
    catch {
      case e: SQLException => con.rollback()
        LOGGER.error("rollback", e)
        throw new RuntimeException("rollback", e)
    }
    finally {
      if (null != con) {
        con.commit()
        con.close()
      }
    }
  }
}