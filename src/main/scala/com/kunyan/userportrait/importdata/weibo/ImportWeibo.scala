package com.kunyan.userportrait.importdata.weibo

import java.sql.DriverManager

import com.kunyan.userportrait.log.PLogger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/9.
  */
object ImportWeibo {

  var count = 0
  val url = "jdbc:mysql://localhost:3306/personas"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "dataservice2016"
  Class.forName(driver)

  val conn = DriverManager.getConnection(url, username, password)
  val insert = "insert into weibo_temp (user_id, weibo_id, times) values (?, ?, ?)"
  val ps = conn.prepareStatement(insert)

  val insertIndex = "insert into main_index (ad, ua) values (?,?)"
  val indexPs = conn.prepareStatement(insertIndex)

  val st = conn.createStatement

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT WEIBO")
    val sc = new SparkContext(sparkConf)

    sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 3).collect.foreach(x => saveToDB(x))

    ps.executeBatch()

    sc.stop()
  }

  def saveToDB(arr: Array[String]): Unit = {

    val ad = arr(0)
    val ua = arr(1)
    val infos = arr(2)

    if (ad.length < 1000 && ua.length < 1000) {

      val id = getMainId(ad, ua)

      infos.split(",").foreach(x => {

        val arr = x.split("->")
        var info = arr(0)
        if (info.startsWith("wb:") || info.startsWith("ss:")) {
          info = info.substring(3)
          val times = arr(1)

          ps.setInt(1, id)
          ps.setString(2, info)
          ps.setString(3, times)
          ps.addBatch()

          count += 1
        }

        if (count % 100 == 0) {
          ps.executeBatch()
          PLogger.warn(s"count: $count")
        }

      })

    } else {
      PLogger.warn(ad + "->" + ua)
    }

  }

  def getMainId(ad: String, ua: String): Int = {

    var id = -1

    var rs = st.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))
    while (rs.next()) {
      id = rs.getInt(1)
    }

    if (id == -1) {

      indexPs.setString(1, ad)
      indexPs.setString(2, ua)
      indexPs.execute()

      rs = st.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))
      while (rs.next()) {
        id = rs.getInt(1)
      }
    }

    id
  }

}
