package com.kunyan.userportrait.db

import java.sql.Connection
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by C.J.YOU on 2016/4/26.
  * 数据库表操作
  */
object Table extends  Serializable {

  // 对应数据库中main_index表结构
  case class MainIndex(phone: String, qq: String, weibo: String)
  // 对应数据库中maimai表结构
  case  class MaiMai(var mainIndex: Int, var phone: String, email: String, job: String, position: String, realName: String, company: String, education: String, address: String )
  // 对应数据库中O2O表结构
  case class WaiMai(var mainIndex: Int, var phone: String, email: String, realName: String, address: String)

  /**
    * @param maiMai  MaiMai实例
    * @return 格式化后用户信息
    */
  def toString(maiMai: MaiMai): String = {

    maiMai.realName + "\t" + maiMai.phone +  "\t" + maiMai.email +  "\t" +  maiMai.position +  "\t" + maiMai.job +  "\t" + maiMai.address +  "\t" + maiMai.company +  "\t" + maiMai.education

  }

  /**
    * @param o2O O2O实例
    * @return 格式化后用户信息
    */
  def toString(o2O: WaiMai): String = {

    o2O.realName + "\t" + o2O.phone +  "\t" + o2O.email + "\t" + o2O.address

  }

  @volatile private var instance: Broadcast[DataFrame] = null

  /**
    * 获取数据库数据生成广播变量
 *
    * @param sqlc SQLContext
    * @return 广播主表数据
    */
  def getInstance(sqlc: SQLContext): Broadcast[DataFrame] = {

    if (instance == null) {

      synchronized {

        if (instance == null) {

          val df = Table.getTableData(sqlc, DBOperation.url, "main_index")
          instance = sqlc.sparkContext.broadcast(df)

        }
      }
    }

    instance

  }

  /**
    * 清空广播变量
    */
  def resetInstance(): Unit ={
    instance = null
  }

  /**
    * 加载数据库数据
 *
    * @param sqlc SQLContext
    * @param readConnection 数据连接字符串
    * @param tableName 表名
    * @return DF
    */
  def getTableData(sqlc:SQLContext, readConnection: String, tableName: String): DataFrame = {

    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    val res = sqlc.read.jdbc(readConnection,tableName,properties)

    res

  }

  /**
    * 写入数据库
 *
    * @param dataFrame  需要写入数据的DF
    * @param writeConnection 连接字符串
    * @param tableName 表名
    */
  def writeTableData(dataFrame: DataFrame, writeConnection: String, tableName: String): Unit = {

    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    dataFrame.write.mode(SaveMode.Append).jdbc(writeConnection,tableName,properties)

  }

  /**
    * 判断数据库是否存在数据
 *
    * @param col 列名
    * @param param 参数
    * @param conn 连接字符串
    * @return 查询的结果
    */
  def isExist(col: String, param: String, conn: Connection): (Int, String, String, String) = {

    val st = conn.createStatement
    var id = -1
    var item1 = ""
    var item2 = ""
    var item3 = ""
    val rs = st.executeQuery(String.format("select id,phone,qq,weibo from main_index where "+ col+" =\"%s\"",param))

    while (rs.next()) {

      id = rs.getInt(1)
      item1 = rs.getString(2)
      item2 = rs.getString(3)
      item3 = rs.getString(4)
    }

    (id,item1,item2,item3)

  }

  /**
    * 判断Maimai数据库是否存在数据
 *
    * @param col 列名
    * @param param 参数
    * @param conn 连接字符串
    * @return 查询的结果
    */
  def isExistMaiMai(col: String, param: String, conn: Connection): (Int, String) = {

    val st = conn.createStatement
    var id = -1
    var item1 = ""

    val rs = st.executeQuery(String.format("select main_index_id,phone from maimai where "+ col+" =\"%s\"",param))

    while (rs.next()) {

      id = rs.getInt(1)
      item1 = rs.getString(2)
    }

    (id,item1)

  }

}
