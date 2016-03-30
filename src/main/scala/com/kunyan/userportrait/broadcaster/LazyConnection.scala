package com.kunyan.userportrait.broadcaster

import java.sql.{DriverManager, PreparedStatement, Statement}

import com.kunyan.userportrait.log.PLogger

/**
  * Created by yangshuai on 2016/3/10.
  */
class LazyConnection(createStatement: () => (PreparedStatement, PreparedStatement, Statement)) extends Serializable {

  lazy val tuple3 = createStatement()
  var count = 0

  def insertWeibo(id: Int, weiboId: String, times: String): Unit = {

    val weiboPs = tuple3._1

    weiboPs.setInt(1, id)
    weiboPs.setString(2, weiboId)
    weiboPs.setString(3, times)
    weiboPs.addBatch()
    count += 1

    if (count % 100 == 0) {
      weiboPs.executeBatch()
      PLogger.warn(s"number: $count")
    }

  }

  def getMainId(ad: String, ua: String): Int = {

    val insertPs = tuple3._2
    val statement = tuple3._3

    var id = -1

    var rs = statement.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))

    while (rs.next()) {
      id = rs.getInt(1)
    }

    if (id == -1) {

      insertPs.setString(1, ad)
      insertPs.setString(2, ua)
      insertPs.execute()

      rs = statement.executeQuery(String.format("select * from main_index where ad=\"%s\" and ua=\"%s\"", ad, ua))
      while (rs.next()) {
        id = rs.getInt(1)
      }
    }

    id
  }
}

object LazyConnection {

  def apply(): LazyConnection = {

    val f = () => {

      val url = "jdbc:mysql://localhost:3306/personas"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "dataservice2016"
      Class.forName(driver)

      val connection = DriverManager.getConnection(url, username, password)

      val insert = "insert into weibo_temp (user_id, weibo_id, times) values (?, ?, ?)"
      val weiboPs = connection.prepareStatement(insert)

      val insertIndex = "insert into main_index (ad, ua) values (?,?)"
      val indexPs = connection.prepareStatement(insertIndex)

      val st = connection.createStatement

      sys.addShutdownHook {
        weiboPs.executeBatch()
        st.close()
        connection.close()
      }

      PLogger.warn("create connection")
      (weiboPs, indexPs, st)
    }

    new LazyConnection(f)

  }
}