package com.kunyan.userportrait.importdata.scheduler

import java.util.Properties

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.db.{Table, DBOperation}
import com.kunyan.userportrait.importdata.extractor.Extractor
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

object Scheduler extends Serializable{

  val sparkConf = new SparkConf()
    // .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  var distinctPhone = new HashSet[String]
  var distinctQQ = new HashSet[String]
  var distinctWeibo = new HashSet[String]
  def main(args: Array[String]): Unit = {
    // 策略一 （离线）
    try {
      val data = sc.textFile(args(0)).persist(StorageLevel.MEMORY_AND_DISK).repartition(3)
      println("start extract user info:")
      val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("weibo.com") || x(3).contains("suning.com") || x(3).contains("qq.com") || x(3).contains("dianping.com") || x(3).contains("4008823823.com.cn"))
        .map(Extractor.extratorUserInfo)
        .filter(filterUserInfo)
        .distinct().persist(StorageLevel.MEMORY_AND_DISK).repartition(3)
      // val insertUserInfo = userInfo.map(divideInsertMap).filter(filterUserInfo).distinct().collect()
      val updateUserInfo = userInfo.map(divideInsertMap).groupByKey()
      val insertUserInfo = updateUserInfo.lookup(0).head.filter(filterUserInfo)
      println("insertIterator size:"+insertUserInfo.size)
      println("INSERTING.........................")
      DBOperation.batchInsert(Array("phone", "qq", "weibo"),insertUserInfo)
      println("INSERTED.........................")
     // val updateUserInfo = userInfo.map(divideUpdateMap).groupByKey().sortByKey(ascending = true)
      val item1 = updateUserInfo.lookup(1)
      val item2 = updateUserInfo.lookup(2)
      val item3 = updateUserInfo.lookup(3)
      println("UPDATING...........................")
      if(item1.nonEmpty){
        val phoneUpdateIterator = item1.head
        println("phoneUpdateIterator size:"+phoneUpdateIterator.size)
        DBOperation.batchUpdate(Array("qq", "weibo"),phoneUpdateIterator)
      }
      if(item2.nonEmpty){
        val qqUpdateIterator = item2.head
        println("qqUpdateIterator size:"+qqUpdateIterator.size)
        DBOperation.batchUpdate(Array("phone", "weibo"),qqUpdateIterator)
      }
      if(item3.nonEmpty) {
        val weiboUpateIterator = item3.head
        println ("weiboUpateIterator size:" + weiboUpateIterator.size)
        DBOperation.batchUpdate (Array ("phone", "qq"), weiboUpateIterator)
      }
      println("updated mysql main_index:")

    } catch {
      case e: Exception =>
      println(e.getMessage)
     } finally {
        DBOperation.connection.close()
      }
    println("end")

  }
  def filterUserInfo(x:(String,String,String)):Boolean ={
    if(x._1.nonEmpty || x._2.nonEmpty || x._3.nonEmpty) true else false
  }
  // insert
  def divideInsertMap(item:(String,String,String)) : (Int,(String,String,String)) = {
    var phone = item._1
    var qq = item._2
    var weibo = item._3
    if (phone.isEmpty) phone = "Nodef"
    if (qq.isEmpty) qq = "Nodef"
    if (weibo.isEmpty) weibo = "Nodef"
    // println("phone:"+phone+",qq:"+qq+",weibo:"+weibo)
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
      (0,(item._1, item._2, item._3))
    } else if(phoneDataFrame._1 != -1){
      val id = phoneDataFrame._1
      if (qq == "Nodef")
        qq = phoneDataFrame._3
      if (weibo == "Nodef")
        weibo = phoneDataFrame._4
      (1,(qq, weibo, id.toString))
    } else if(qqDataFrame._1 != -1){
      val id = qqDataFrame._1
      if (phone == "Nodef")
        phone = qqDataFrame._2
      if (weibo == "Nodef")
        weibo = qqDataFrame._4
      (2,(phone, weibo, id.toString))
    }else if(weiboDataFrame._1 != -1){
      val id = weiboDataFrame._1
      if (phone == "Nodef")
        phone = weiboDataFrame._2
      if (qq == "Nodef")
        qq = weiboDataFrame._3
      (3,(phone, qq, id.toString))
    } else (0,("","",""))

  }
  // update
  def divideUpdateMap(item:(String,String,String)) : ((Int,(String,String,String))) = {
    var phone = item._1
    // println(phone)
    var qq = item._2
    // println(qq)
    var weibo = item._3
    // println(weibo)
    if (phone.isEmpty) phone = "Nodef"
    if (qq.isEmpty) qq = "Nodef"
    if (weibo.isEmpty) weibo = "Nodef"
    val phoneData = Table.isExist("phone",phone,DBOperation.connection)
    // println(phoneData)
    val qqData = Table.isExist("qq",qq,DBOperation.connection)
    // println(qqData)
    val weiboData = Table.isExist("weibo",weibo,DBOperation.connection)
    // println(weiboData)
    if ( phoneData._1 > 0) {
      val id = phoneData._1
      if (qq == "Nodef")
        qq = phoneData._3
      if (weibo == "Nodef")
        weibo = phoneData._4
      (0,(qq, weibo, id.toString))
    } else if (qqData._1 > 0) {
      val id = qqData._1
      if (phone == "Nodef")
        phone = qqData._2
      if (weibo == "Nodef")
        weibo = qqData._4
      (1,(phone, weibo, id.toString))
    } else if (weiboData._1 > 0 ) {
      val id = weiboData._1
      if (phone == "Nodef")
        phone = weiboData._2
      if (qq == "Nodef")
        qq = weiboData._3
      (2,(phone, qq, id.toString))
    } else {
      (3,(item._1, item._2, item._3))
    }
  }

  // readData
  def getTableData(readConnection:String,tableName:String): Unit ={
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sqlContext.read.jdbc(readConnection,tableName,properties).registerTempTable(tableName)
  }
}
