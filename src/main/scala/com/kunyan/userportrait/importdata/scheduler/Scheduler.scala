package com.kunyan.userportrait.importdata.scheduler

import java.util.Properties

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
    val sparkConf = new SparkConf()
      // .setMaster("local")
      .setAppName("USER")
      .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
      .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // val ssc = new StreamingContext()
    import  sqlContext.implicits._
    def main(args: Array[String]): Unit = {
      // 策略一
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
        //  val userInfo = sc.parallelize(Seq(("3","3","3weibo")))
        val insertUserInfo = userInfo.map(divideInsertMap).filter(filterUserInfo).distinct().collect()
        println("insertIterator size:"+insertUserInfo.length)
        println("INSERTING.........................")
        DBOperation.batchInsert(Array("phone", "qq", "weibo"),insertUserInfo)
        val updateUserInfo = userInfo.map(divideUpdateMap).groupByKey().sortByKey(ascending = true)
        val item1 = updateUserInfo.lookup(0)
        val item2 = updateUserInfo.lookup(1)
        val item3 = updateUserInfo.lookup(2)
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
            val id = phoneDataFrame.take(1)(0)(0).toString
            if (qq == "Nodef")
              qq = phoneDataFrame.take(1)(0)(1).toString
            if (weibo == "Nodef")
              weibo = phoneDataFrame.take (1)(0)(2).toString
            // updataColumn (sqlContext, Array ("qq", "weibo"), Array (qq, weibo, id))
            phoneSameListBuffer.+= ((qq, weibo, id))
          } else if (qqDataFrame.count > 0) {
            val id = qqDataFrame.take(1)(0)(0).toString
            if (phone == "Nodef")
              phone = qqDataFrame.take(1)(0)(1).toString
            if (weibo == "Nodef")
              weibo = qqDataFrame.take(1)(0)(2).toString
            // updataColumn (sqlContext, Array ("phone", "weibo"), Array (phone, weibo, id))
            qqSameListBuffer.+= ((phone, weibo, id))

          } else if (weiboDataFrame.count > 0) {
            val id = weiboDataFrame.take(1)(0)(0).toString
            if (phone == "Nodef")
              phone = weiboDataFrame.take(1)(0)(1).toString
            if (qq == "Nodef")
              qq = weiboDataFrame.take(1)(0)(2).toString
            // updataColumn (sqlContext, Array ("phone", "qq"), Array (phone, qq, id))
            weiboSameListBuffer.+= ((phone, qq, id))
          } else {
            if (item._1.nonEmpty || item._2.nonEmpty || item._3.nonEmpty)
            // insertColumn (sqlContext, Array ("phone", "qq", "weibo"), Array (item._1, item._2, item._3))
              noneSameBuffer.+= ((item._1, item._2, item._3))
          }
        }
      } catch {
        case e:Exception =>
          println("updateMainIndex error")
      }
      (phoneSameListBuffer,qqSameListBuffer,weiboSameListBuffer,noneSameBuffer)
    }
    // insert
    def divideInsertMap(item:(String,String,String)) : ((String,String,String)) = {
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
      if ( phoneDataFrame._1 == -1 && qqDataFrame._1 == -1 && weiboDataFrame._1 == -1) {
        (item._1, item._2, item._3)
      } else  ("","","")
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
}
