package com.kunyan.userportrait.importdata.scheduler

import java.util.Properties

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.db.{Table, DBOperation}
import com.kunyan.userportrait.importdata.extractor.Extractor
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
* Created by C.J.YOU on 2016/4/26.
* 用户画像分析手机，qq，微博号唯一标识的主程序
*/
object Scheduler extends Serializable {

  val sparkConf = new SparkConf()
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)

  var distinctPhone = new HashSet[String]

  var distinctQQ = new HashSet[String]

  var distinctWeibo = new HashSet[String]

  def main(args: Array[String]): Unit = {

    try {

      val data = sc.textFile(args(0)).persist(StorageLevel.MEMORY_AND_DISK).repartition(3)
      println("start extract user info:")
      val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("weibo.com") || x(3).contains("suning.com") || x(3).contains("qq.com") || x(3).contains("dianping.com") || x(3).contains("4008823823.com.cn"))
        .map(Extractor.extractorUserInfo)
        .filter(filterUserInfo)
        .distinct().persist(StorageLevel.MEMORY_AND_DISK).repartition(3)
      val updateUserInfo = userInfo.map(divideInsertMap).groupByKey()
      val insertUserInfo = updateUserInfo.lookup(0).head.filter(filterUserInfo)
      println("insertIterator size:" + insertUserInfo.size)
      println("INSERTING.........................")
      DBOperation.batchInsert(Array("phone", "qq", "weibo"), insertUserInfo)
      println("INSERTED.........................")
      val updatePhone = updateUserInfo.lookup(1)
      val updateQQ = updateUserInfo.lookup(2)
      val updateWeiBo = updateUserInfo.lookup(3)
      println("UPDATING...........................")

      if(updatePhone.nonEmpty){

        val phoneUpdateIterator = updatePhone.head
        println("phoneUpdateIterator size:" + phoneUpdateIterator.size)
        DBOperation.batchUpdate(Array("qq", "weibo"), phoneUpdateIterator)

      }

      if(updateQQ.nonEmpty){

        val qqUpdateIterator = updateQQ.head
        println("qqUpdateIterator size:" + qqUpdateIterator.size)
        DBOperation.batchUpdate(Array("phone", "weibo"), qqUpdateIterator)

      }

      if(updateWeiBo.nonEmpty) {

        val weiboUpateIterator = updateWeiBo.head
        println ("weiboUpateIterator size:" + weiboUpateIterator.size)
        DBOperation.batchUpdate (Array ("phone", "qq"), weiboUpateIterator)

      }

      println("updated mysql main_index:")

    } catch {

      case e: Exception => println(e.getMessage)

     } finally {

        DBOperation.connection.close()

      }

    println("end")

  }
  def filterUserInfo(x: (String, String, String)): Boolean = {

    if(x._1.nonEmpty || x._2.nonEmpty || x._3.nonEmpty) {
      true
    } else {
      false
    }
  }

  /**
    * 用户信息解析函数
    *
    * @param item  用户信息的元组（phone，qq，weibo）
    * @return  解析后用户信息
    */
  def divideInsertMap(item: (String, String, String)) : (Int, (String, String, String)) = {

    var phone = item._1
    var qq = item._2
    var weibo = item._3

    if (phone.isEmpty) {
      phone = "Nodef"
    }

    if (qq.isEmpty) {
      qq = "Nodef"
    }

    if (weibo.isEmpty) {
      weibo = "Nodef"
    }
    val phoneDataFrame = Table.isExist("phone",phone,DBOperation.connection)
    val qqDataFrame = Table.isExist("qq",qq,DBOperation.connection)
    val weiboDataFrame = Table.isExist("weibo",weibo,DBOperation.connection)
    val noExistInTable = phoneDataFrame._1 == -1 && qqDataFrame._1 == -1 && weiboDataFrame._1 == -1
    val isDistinctPhone = distinctPhone.contains(phone)
    val isDistinctQQ = distinctQQ.contains(qq)
    val isDistinctWeibo = distinctWeibo.contains(weibo)

    if (noExistInTable && !isDistinctPhone && !isDistinctQQ && !isDistinctWeibo) {

      distinctPhone = distinctPhone.+(phone)
      distinctQQ = distinctQQ.+(qq)
      distinctWeibo = distinctWeibo.+(weibo)

      (0, (item._1, item._2, item._3))

    } else if(phoneDataFrame._1 != -1) {

      val id = phoneDataFrame._1

      if (qq == "Nodef") {
        qq = phoneDataFrame._3
      }
      if (weibo == "Nodef") {
        weibo = phoneDataFrame._4
      }

      (1, (qq, weibo, id.toString))

    } else if(qqDataFrame._1 != -1) {

      val id = qqDataFrame._1

      if (phone == "Nodef") {
        phone = qqDataFrame._2
      }

      if (weibo == "Nodef") {
        weibo = qqDataFrame._4
      }

      (2, (phone, weibo, id.toString))

    }else if(weiboDataFrame._1 != -1) {

      val id = weiboDataFrame._1

      if (phone == "Nodef") {
        phone = weiboDataFrame._2
      }

      if (qq == "Nodef") {
        qq = weiboDataFrame._3
      }

      (3, (phone, qq, id.toString))

    } else {
      (0, ("", "", ""))
    }
  }
}
