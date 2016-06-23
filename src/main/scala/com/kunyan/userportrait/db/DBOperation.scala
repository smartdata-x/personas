package com.kunyan.userportrait.db

import java.sql.{DriverManager, PreparedStatement}

import com.kunyan.userportrait.db.Table.{MaiMai, WaiMai}
import com.kunyan.userportrait.log.PLogger
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/4/26.
  */
object DBOperation extends Serializable {

  val url = "jdbc:mysql://xxxx:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8"
  val driver = "com.mysql.jdbc.Driver"
  val username = "personas"
  val password = "personas"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  val listBuffer = new ListBuffer[(String, String, String)]

  /**
    * 数据插入到 main_index
    * @param columns 列名
    * @param iterator 需插入的数据迭代器
    */
  def batchInsert(columns:Array[String], iterator: Iterable[(String, String,String)]): Unit = {

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

      case e: Exception => PLogger.warn("Mysql Exception")

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * 数据更新到 main_index
    * @param columns 列名
    * @param iterator 需更新的数据迭代器
    */
  def batchUpdate(columns:Array[String], iterator: Iterable[(String, String,String)]): Unit = {

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

      case e: Exception => PLogger.warn("Mysql Exception")

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * 插入和main_index 表中手机号码有关联的数据
    * @param list 与数据库personas中maimai表对应的case类
    */
  def maiMaiInsert(list: ListBuffer[(MaiMai,String)]): Unit = {

    var ps: PreparedStatement = null
    val sql ="insert into maimai(main_index_id,phone,email,job,position,real_name,company,education,address) values (?,?,?,?,?,?,?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      list.foreach { x =>

        val maiMai = x._1
        ps.setInt(1,maiMai.mainIndex)
        ps.setString(2,maiMai.phone.toString)
        ps.setString(3,maiMai.email)
        ps.setString(4,maiMai.job)
        ps.setString(5,maiMai.position)
        ps.setString(6,maiMai.realName)
        ps.setString(7,maiMai.company)
        ps.setString(8,maiMai.education)
        ps.setString(9,maiMai.address)
        ps.addBatch()

      }

      ps.executeBatch()

    } catch {

      case e: Exception => PLogger.warn("Mysql:"+e.getMessage)

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * 插入和main_index 表中手机号码有关联的数据
    * @param arr 与数据库personas中maimai表对应的case类
    */
  def maiMaiInsert(arr: Array[(MaiMai,String)]): Unit = {

    var ps: PreparedStatement = null
    val sql = "insert into maimai(main_index_id,phone,email,job,position,real_name,company,education,address) values (?,?,?,?,?,?,?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      arr.foreach { x =>

        val maiMai = x._1
        ps.setInt(1, maiMai.mainIndex)
        ps.setString(2, maiMai.phone.toString)
        ps.setString(3, maiMai.email)
        ps.setString(4, maiMai.job)
        ps.setString(5, maiMai.position)
        ps.setString(6, maiMai.realName)
        ps.setString(7, maiMai.company)
        ps.setString(8, maiMai.education)
        ps.setString(9, maiMai.address)
        ps.addBatch()

      }

      ps.executeBatch()

    } catch {

      case e: Exception => PLogger.warn("Mysql:" + e.getMessage)

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * 插入与main_index 表没有手机号码关联的数据
    * @param list 与数据库personas中maimai_tmp表对应的case类
    */
  def maiMaiTempInsert(list: ListBuffer[(MaiMai,String)]): Unit = {

    var ps: PreparedStatement = null
    val sql ="insert into maimai_tmp (phone,email,job,position,real_name,company,education,address) values (?,?,?,?,?,?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      list.foreach { x =>

        val maiMai = x._1
        ps.setString(1,maiMai.phone.toString)
        ps.setString(2,maiMai.email)
        ps.setString(3,maiMai.job)
        ps.setString(4,maiMai.position)
        ps.setString(5,maiMai.realName)
        ps.setString(6,maiMai.company)
        ps.setString(7,maiMai.education)
        ps.setString(8,maiMai.address)
        ps.addBatch()

      }

      ps.executeBatch()

    } catch {

      case e: Exception => PLogger warn "Mysql:" + e.getMessage

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * o2o 平台数据的更新
    * @param list 用户信息数组
    * @param typeTag o2o平台分类标签
    */
  def waiMaiInsert(list: ListBuffer[(WaiMai,String)], typeTag: Int): Unit = {

    var ps: PreparedStatement = null
    val sql = "insert into O2O(main_index_id,phone,email,real_name,address,platform) values (?,?,?,?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      list.foreach { x =>

        val waiMai = x._1
        ps.setInt(1, waiMai.mainIndex)
        ps.setString(2, waiMai.phone.toString)
        ps.setString(3, waiMai.email)
        ps.setString(4, waiMai.realName)
        ps.setString(5, waiMai.address)
        ps.setInt(6, typeTag)
        ps.addBatch()
      }
      ps.executeBatch()

    } catch {

      case e: Exception => PLogger.warn("Mysql:" + e.getMessage)

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * o2o 平台数据的更新
    * @param list 用户信息数组
    * @param typeTag o2o平台分类标签
    */
  def waiMaiInsert(list: Array[(WaiMai,String)], typeTag: Int): Unit = {

    var ps: PreparedStatement = null
    val sql = "insert into O2O(main_index_id,phone,email,real_name,address,platform) values (?,?,?,?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      list.foreach { x =>

        val waiMai = x._1
        ps.setInt(1, waiMai.mainIndex)
        ps.setString(2, waiMai.phone.toString)
        ps.setString(3, waiMai.email)
        ps.setString(4, waiMai.realName)
        ps.setString(5, waiMai.address)
        ps.setInt(6, typeTag)
        ps.addBatch()

      }

      ps.executeBatch()

    } catch {

      case e: Exception => PLogger.warn("Mysql:" + e.getMessage)

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * o2o 临时数据表更新
    * @param list 用户数据集合
    */
  def waiMaiTmpInsert(list: ListBuffer[(WaiMai,String)]): Unit = {

    var ps: PreparedStatement = null
    val sql = "insert into O2O_tmp (phone,email,real_name,address,platform) values (?,?,?,?,?)"
    ps = connection.prepareStatement(sql)

    try {

      list.foreach { x =>

        val waiMai = x._1
        ps.setString(1, waiMai.phone.toString)
        ps.setString(2, waiMai.email)
        ps.setString(3, waiMai.realName)
        ps.setString(4, waiMai.address)
        ps.setInt(5, 0)
        ps.addBatch()

      }

      ps.executeBatch()

    } catch {

      case e: Exception => PLogger.warn("Mysql:" + e.getMessage)

    } finally {

      if (ps != null) {
        ps.close()
      }
    }
  }

  /**
    * @param sparkContext sc
    * @param path 读取文件路径
    * @param tableName 表名
    * @param tagColumn 列名
    */
  def mergeTemp(sparkContext:SparkContext,path: String, tableName: String, tagColumn: String): Unit = {

      val res = sparkContext.textFile(path).map { x =>
        val arr = x.split(",")
        val phone = arr(0).replace("\"", "")
        val email = arr(1).replace("\"", "")
        val job = arr(2).replace("\"", "")
        val pos = arr(3).replace("\"", "")
        val real = arr(4).replace("\"", "")
        val com = arr(5).replace("\"", "")
        val edu = arr(6).replace("\"", "")
        val add = arr(7).replace("\"", "")
        (phone, email, job, pos, real, com, edu, add)
      }.cache()

      PLogger.warn("count:" + res.count())
      var index = 1
      res.foreach { row =>
        PLogger.warn("index:"+ index)
        index += 1
        val phone = row._1.toString
        val  id = Table.isExist(tagColumn,phone,connection)._1
        if(id == -1){
          listBuffer.+=((phone,"",""))
        }
      }

    PLogger.warn("filter1 over: "+ listBuffer.size)
    batchInsert(Array("phone","qq","weibo"), listBuffer)
    PLogger.warn("batchInsert over")

    val insert = res.map { x =>
      val phone = x._1
      val  id = Table.isExist(tagColumn,phone,connection)._1
      (id,phone,x._2,x._3,x._4,x._5,x._6,x._7,x._8)
    }.filter(_._1 != -1).map { x =>
      val maimai = MaiMai(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9)
      (maimai,"")
    }.collect()

    PLogger.warn("filter2 over:" + insert.length)
    maiMaiInsert(insert)
    PLogger.warn("maimai over")

  }

}
