package com.kunyan.userportrait.importdata.crawler

import java.util.concurrent.Executors

import com.kunyan.userportrait.config.{PlatformConfig, SparkConfig}
import com.kunyan.userportrait.db.{DBOperation, Table}
import com.kunyan.userportrait.db.Table.O2O
import com.kunyan.userportrait.importdata.crawler.task.MeiTuanTask
import com.kunyan.userportrait.log.PLogger
import com.kunyan.userportrait.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/5/21.
  * 美团网用户信息解析主类
  */
object MeiTuanScheduler {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    .set("spark.driver.allowMultipleContexts","true")

  val sc = new SparkContext(sparkConf)
  val userInfoList = new ListBuffer[String]()
  val userMeiTuanList = new ListBuffer[(O2O,String)]()

  def main(args: Array[String]) {

    val thread = 5
    val es = Executors.newFixedThreadPool(thread)
    val rdd = sc.textFile(args(0))
    val data = rdd.distinct()
    PLogger.warn("start meituan extract user info:")
    val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("meituan.com"))
        .map(x =>(x(2),x(6)))
        .distinct()
         .collect()

    PLogger.warn("userinfo size:" + userInfo.length)

    try {

      var index = 0

      for(index <- userInfo.indices) {

        PLogger.warn(index + "of\t" + userInfo.length)
        val x = userInfo(index)
        PLogger.warn(x._1+"\t" + x._2 + "\t")
        val task = new MeiTuanTask(x._1, x._2)
        val future = es.submit(task)
        val meiTuan = getMeiTuanMainIndex(future.get()._2)
        userMeiTuanList.+=((meiTuan._1,meiTuan._2))
        userInfoList.+=(meiTuan._2)

        PLogger.warn("解析完毕，开始写入数据库..................")
        val exist = userMeiTuanList.filter(x => x._1.mainIndex != -1)
        val noExist = userMeiTuanList.filter(x => x._1.mainIndex == -1).filter(x => x._1.phone != "Nodef")
        /** sync main_index **/
        val updateMain = noExist.map(x =>(x._1.phone,"",""))
        DBOperation.batchInsert(Array("phone","qq","weibo"),updateMain)
        DBOperation.O2OInsert(exist,PlatformConfig.MEITUAN)
        /** 不更新tmp表  **/
        val newExist = noExist.map(x => {
          x._1.mainIndex = Table.isExist("phone",x._1.phone,DBOperation.connection)._1
          x
        }).filter(_._1.mainIndex != -1)
        DBOperation.O2OInsert(newExist,PlatformConfig.MEITUAN)
        PLogger.warn("写入数据库完毕..................:" + newExist.size)

      }

    } catch {

      case e:Exception => PLogger.warn("task error")

    } finally {

      FileUtil.writeToFile(args(1),userInfoList.distinct.toArray)
      PLogger.warn("写入文件完毕..................")
      es.shutdown()
      DBOperation.connection.close()

    }

  }

  /**
    * 获取main_index 中的id号
    * @param map 解析好的数据map集合
    * @return  返回一个case类和数据库表对应, 格式化后用户信息字符串
    */
  def getMeiTuanMainIndex(map: scala.collection.mutable.HashMap[String,String]): (O2O, String) = {

    var info = ""
    var o2o: O2O = null
    val phone = if(map.contains("手机号") && !map.get("手机号").get.contains("*")) map.get("手机号").get else "Nodef"
    val mainIndex = Table.isExist("phone",phone,DBOperation.connection)
    val mainIndexId = mainIndex._1
    val email = if(map.contains("邮箱")) map.get("邮箱").get else ""
    val realName = if(map.contains("姓名")) map.get("姓名").get else ""
    val address = if(map.contains("地址")) map.get("地址").get else "Nodef"
    o2o = O2O(mainIndexId, phone, email, realName, address)
    info = realName + "\t" + phone +  "\t"  + address +  "\t" +email

    (o2o,info)

  }

}
