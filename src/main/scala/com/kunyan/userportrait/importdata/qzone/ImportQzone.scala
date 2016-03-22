package com.kunyan.userportrait.importdata.qzone

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.kunyan.userportrait.log.PLogger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/2.
  */
object ImportQzone {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    sc.textFile(args(0)).map(_.split("\t")).foreachPartition(saveToDB)

    sc.stop()
  }

  def saveToDB(iterator: Iterator[Array[String]]): Unit = {

    var conn: Connection = null
    var ps: PreparedStatement = null

    val sql = "insert into qq_temp (user_id, qq, times) values (?, ?, ?)"
    try {

      val url = "jdbc:mysql://localhost:3306/personas"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "dataservice2016"
      Class.forName(driver)

      conn = DriverManager.getConnection(url, username, password)
      ps = conn.prepareStatement(sql)

      iterator.foreach(arr => {

        val ad = arr(0)
        val ua = arr(1)
        val infos = arr(2)

        if (ad.length < 1000 && ua.length < 1000) {

          val id = getMainId(ad, ua, conn)

          infos.split(",").foreach(x => {
            val arr = x.split("->")
            val qq = arr(0)
            val times = arr(1)
            if (qq.length <= 20 && (qq forall Character.isDigit)) {

              ps.setInt(1, id)
              ps.setString(2, qq)
              ps.setString(3, times)

              ps.addBatch()
            } else {
              PLogger.warn(ad + "->" + ua + "->" + infos)
            }
          })

          ps.executeBatch()
        } else {
          PLogger.warn(ad + "->" + ua + "->" + infos)
        }
      })

    } catch {
      case e: Exception => println("Mysql Exception")
        PLogger.exception(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }

  }

  def getMainId(ad: String, ua: String, conn: Connection): Int = {

    val st = conn.createStatement
    var id = -1

    var rs = st.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))
    while (rs.next()) {
      id = rs.getInt(1)
    }

    if (id == -1) {
      val insertIndex = "insert into main_index (ad, ua) values (?,?)"
      val ps = conn.prepareStatement(insertIndex)
      ps.setString(1, ad)
      ps.setString(2, ua)
      ps.execute()
    }

    rs = st.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))
    while (rs.next()) {
      id = rs.getInt(1)
    }

    id
  }

}
