package com.kunyan.userportrait.importdata

import java.io.PrintWriter
import java.sql.DriverManager

/**
  * Created by yangshuai on 2016/3/28.
  */
object ImportEmailFromDB {

  val url = "jdbc:mysql://localhost:3306/personas"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "dataservice2016"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)

  def main(args: Array[String]): Unit = {

    val statement = connection.createStatement()
    val selectMulti = "select a.ad, b.qq from qq b join main_index a on a.id=b.user_id"
    val rs = statement.executeQuery(selectMulti)
    val writer = new PrintWriter("E:\\qq.data", "UTF-8")

    while (rs.next()) {
      val ad = rs.getString(1)
      val qq = rs.getString(2)

      writer.println(ad + "\t" + qq)
    }

    writer.close()
  }

}
