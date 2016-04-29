package com.kunyan.userportrait.importdata.scheduler

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.db.{DBOperation, Table}
import com.kunyan.userportrait.importdata.extractor.Extractor
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Scheduler extends Serializable{

  val sparkConf = new SparkConf()
    // .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
 // val ssc = new StreamingContext()

  def main(args: Array[String]): Unit = {
    // 策略一
    try {
      // 读取现有数据库数据（index 表）
      Table.getTableData (sqlContext, "jdbc:mysql://222.73.34.91:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8", "main_index")
      val dbdataInit = sqlContext.sql("select count(*) from main_index").count()
      println("dbdata size:" + dbdataInit)
      val data = sc.textFile(args(0)).persist(StorageLevel.MEMORY_AND_DISK).repartition(3)
      println("start extract user info:")
      val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("weibo.com") || x(3).contains("suning.com") || x(3).contains("qq.com") || x(3).contains("dianping.com") || x(3).contains("4008823823.com.cn"))
        .map(Extractor.extratorUserInfo)
        .filter(filterUserInfo)
        .distinct()
        .collect()
      println("updating.........")
      // 先插入后更新
      val dbData = updateMainIndex(userInfo)
      DBOperation.batchInsert(Array("phone", "qq", "weibo"),dbData._4.toIterator)
      Table.getTableData (sqlContext, "jdbc:mysql://222.73.34.91:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8", "main_index")
      val dbdata2 = sqlContext.sql("select count(*) from main_index").count()
      println("dbdata size:" + dbdata2 )
      DBOperation.batchUpdate(Array("qq", "weibo"),dbData._1.toIterator)
      DBOperation.batchUpdate(Array("phone", "weibo"),dbData._2.toIterator)
      DBOperation.batchUpdate(Array("phone", "qq"),dbData._3.toIterator)

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
  // 获取最新需要更新的数据
  def updateMainIndex(data: Array[(String,String,String)]): (ListBuffer[(String,String,String)],ListBuffer[(String,String,String)],ListBuffer[(String,String,String)],ListBuffer[(String,String,String)]) ={
    // println("updateMainIndex:")
    val  phoneSameListBuffer = new ListBuffer[(String,String,String)]
    val  qqSameListBuffer = new ListBuffer[(String,String,String)]
    val  weiboSameListBuffer = new ListBuffer[(String,String,String)]
    val  noneSameBuffer = new ListBuffer[(String,String,String)]
    try {
       for(item <- data) {
         println ("item:" + item)
         // Table.getTableData (sqlContext, "jdbc:mysql://222.73.34.91:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8", "main_index")
         var phone = item._1
         var qq = item._2
         var weibo = item._3
         if (phone.isEmpty)
           phone = "Nodef"
         if (qq.isEmpty)
           qq = "Nodef"
         if (weibo.isEmpty)
           weibo = "Nodef"
         val phoneDataFrame = sqlContext.sql ("select id,qq,weibo from main_index where phone ='" + phone + "'")
         println ("phoneDataFrame:" + phoneDataFrame.count)
         val qqDataFrame = sqlContext.sql ("select id,phone,weibo from main_index where qq ='" + qq + "'")
         println ("qqDataFrame:" + qqDataFrame.count)
         val weiboDataFrame = sqlContext.sql ("select id,phone,qq from main_index where weibo ='" + weibo + "'")
         println ("weiboDataFrame:" + weiboDataFrame.count)
         if (phoneDataFrame.count > 0) {
           val id = phoneDataFrame.take (1)(0)(0).toString
           if (qq == "Nodef")
             qq = phoneDataFrame.take (1)(0)(1).toString
           if (weibo == "Nodef")
             weibo = phoneDataFrame.take (1)(0)(2).toString
           phoneSameListBuffer.+= ((qq, weibo, id))
         } else if (qqDataFrame.count > 0) {
           val id = qqDataFrame.take (1)(0)(0).toString
           if (phone == "Nodef")
             phone = qqDataFrame.take (1)(0)(1).toString
           if (weibo == "Nodef")
             weibo = qqDataFrame.take (1)(0)(2).toString
           qqSameListBuffer.+= ((phone, weibo, id))

         } else if (weiboDataFrame.count > 0) {
           val id = weiboDataFrame.take (1)(0)(0).toString
           if (phone == "Nodef")
             phone = weiboDataFrame.take (1)(0)(1).toString
           if (qq == "Nodef")
             qq = weiboDataFrame.take (1)(0)(2).toString
           weiboSameListBuffer.+= ((phone, qq, id))
         } else {
           if (item._1.nonEmpty || item._2.nonEmpty || item._3.nonEmpty)
             noneSameBuffer.+= ((item._1, item._2, item._3))
         }
       }
    } catch {
      case e:Exception =>
        println("updateMainIndex error")
    }
    (phoneSameListBuffer,qqSameListBuffer,weiboSameListBuffer,noneSameBuffer)
  }
}
