package com.kunyan.userportrait.db

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.sql.SQLContext

/**
  * Created by C.J.YOU on 2016/4/26.
  * 数据库操作类
  */
object DBOperation extends Serializable {

  val url = "jdbc:mysql://:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8"
  val driver = "com.mysql.jdbc.Driver"
  val username = "personas"
  val password = "personas"

  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)

  /**
    * 批量擦入数据库
    * @param columns 指定列
    * @param iterator  指定插入数据
    */
  def batchInsert(columns: Array[String], iterator: Iterable[(String, String,String)]): Unit = {

    var ps: PreparedStatement = null
    val sql ="insert into main_index (" + columns (0) + "," + columns (1) + "," + columns (2) + ") values (?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      iterator.foreach(data => {

        ps.setString (1,data._1)
        ps.setString (2,data._2)
        ps.setString (3,data._3)
        ps.addBatch()
      })
      ps.executeBatch()

    } catch {

      case e: Exception => println("Mysql Exception")

    } finally {

      if (ps != null) {

        ps.close()

      }
    }
  }

  /**
    * 批量更新数据库
    * @param columns 指定列
    * @param iterator 指定更新数据
    */
  def batchUpdate(columns: Array[String], iterator: Iterable[(String, String,String)]): Unit = {

    var ps: PreparedStatement = null
    val sql = "update main_index set " + columns (0) + " = ?," + columns (1) + "= ? where id = ?"

    try {

      iterator.foreach(data => {

        ps = connection.prepareStatement(sql)
        ps.setString(1,data._1)
        ps.setString (2,data._2)
        ps.setInt(3,data._3.toInt)
        ps.addBatch()

      })
      ps.executeBatch()

    } catch {

      case e: Exception => println("Mysql Exception")

    } finally {

      if (ps != null) {

        ps.close()

      }
    }
  }
}
