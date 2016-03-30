package com.kunyan.userportrait.importdata.suning

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.kunyan.userportrait.log.PLogger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/2.
  */
object ImportSuning {

  var conn: Connection = null
  var ps: PreparedStatement = null

  val insertPhone = "insert into phone (user_id, phone, times) values (?, ?, ?)"
//  val insertMail = "insert into mail (user_id, mail, times) values (?, ?, ?)"

  val url = "jdbc:mysql://localhost:3306/personas"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "dataservice2016"
  Class.forName(driver)

  conn = DriverManager.getConnection(url, username, password)
  ps = conn.prepareStatement(insertPhone)


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT SUNING")
    val sc = new SparkContext(sparkConf)

    val input = sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 3).collect()

    input.foreach(saveToDB)

    sc.stop()
  }

  def saveToDB(arr: Array[String]): Unit = {

    var count = 0

    val ad = arr(0)
    val ua = arr(1)
    val infos = arr(2)

    if (ad.length < 1000 && ua.length < 1000) {

      val id = getMainId(ad, ua)

      infos.split(",").foreach(x => {

        val arr = x.split("->")
        val info = arr(0)
        val times = arr(1)

        /*if (info.contains("@")) {

          ps.setInt(1, id)
          ps.setString(2, info)
          ps.setString(3, times)
          ps.addBatch()
          count += 1

          if (count % 100 == 0) {
            PLogger.warn(count + "mails")
            ps.executeBatch()
          }*/

        if (info forall Character.isDigit) {

          if (info.length == 11 && info.charAt(0) == '1' && (info.charAt(1) == '3' || info.charAt(1) == '4'|| info.charAt(1) == '5' || info.charAt(1) == '7' || info.charAt(1) == '8')) {

            ps = conn.prepareStatement(insertPhone)
            ps.setInt(1, id)
            ps.setString(2, info)
            ps.setString(3, times)
            ps.addBatch()
            count += 1

            if (count % 500 == 0) {
              PLogger.warn(count + " phone number")
              ps.executeBatch()
            }

          } else {
            PLogger.warn(info)
          }
        }

      })

    } else {
      PLogger.warn(ad + "->" + ua)
    }

    if (ps != null) {
      ps.executeBatch()
    }

  }

  def getMainId(ad: String, ua: String): Int = {

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

      rs = st.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))
      while (rs.next()) {
        id = rs.getInt(1)
      }
    }

    id
  }

}
