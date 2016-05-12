package com.kunyan.userportrait.db

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by C.J.YOU on 2016/4/26.
  * 内存数据表操作类
  */
object Table extends  Serializable {

  /**
    * 获取数据库表数据
    * @param sqlc  SQLContext
    * @param readConnection 数据库连接参数
    * @param tableName 数据库表名
    */
  def getTableData(sqlc: SQLContext, readConnection: String, tableName: String): Unit = {

    val properties = new Properties()
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    sqlc.read.jdbc(readConnection, tableName, properties).registerTempTable(tableName)

  }

  /**
    * 写入内存DataFrame 数据到数据库中
    * @param dataFrame 数据DataFrame
    * @param writeConnection  数据库连接参数
    * @param tableName 数据库表
    */
  def writeTableData(dataFrame: DataFrame, writeConnection: String, tableName: String): Unit = {

    val properties = new Properties()
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    dataFrame.write.mode(SaveMode.Append).jdbc(writeConnection, tableName, properties)

  }

  /**
    * 判断数据库中是否存在数据
    * @param col 指定查询的列
    * @param param 查询参数数据
    * @param conn ：连接数据的对象
    * @return 查询结果
    */
  def isExist(col: String, param: String, conn: Connection): (Int, String, String, String) = {

    val st = conn.createStatement
    var id = -1
    var phone = ""
    var qq = ""
    var weibo = ""
    val rs = st.executeQuery(String.format("select id,phone,qq,weibo from main_index where "+ col+" =\"%s\"",param))

    while (rs.next()) {

      id = rs.getInt(1)
      phone = rs.getString(2)
      weibo = rs.getString(3)
      weibo = rs.getString(4)
    }

    (id, phone, weibo, weibo)

  }

  // get already exist item in db
  def getExistItem(col: Array[String], param: String, conn: Connection): (String, String) = {

    val statement = conn.createStatement()
    var columnOne = ""
    var columnTwo = ""
    val rs = statement.executeQuery(String.format("select "+ col(0) + "," + col(1) +" from main_index where id =\"%s\"", param))

    while (rs.next()) {

       columnOne = rs.getString(1)
       columnTwo = rs.getString(2)
    }

    (columnOne, columnTwo)

  }

}
