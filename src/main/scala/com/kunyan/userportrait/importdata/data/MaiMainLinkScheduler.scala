package com.kunyan.userportrait.importdata.data

import java.util.concurrent.Executors

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.db.Table.MaiMai
import com.kunyan.userportrait.importdata.crawler.task.MaiMaiSubRunable
import com.kunyan.userportrait.importdata.extractor.Extractor
import com.kunyan.userportrait.log.PLogger
import com.kunyan.userportrait.util.{FileUtil, StringUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/2.
  */
object MaiMainLinkScheduler {

  val sparkConf = new SparkConf()
    .setAppName("maimaiLinks")
    .set("spark.serializer", SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max", SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)
  val userInfoList = new ListBuffer[String]()
  val userMaiMaiList = new ListBuffer[(MaiMai, String)]()
  val userMapListBuffer = new ListBuffer[(String, String, mutable.HashMap[String, String])]()

  def main(args: Array[String]) {

    val thread = 6

    val es = Executors.newFixedThreadPool(thread)

    val Array(dataPath, savePath, typeString) = args

    val rdd = sc.textFile(dataPath)
    val data = rdd.distinct()
    PLogger.warn("start extract user info:")
    var userInfo : Array[(String, String, String, String)] = null

    typeString match {

      case "shdx" =>
        userInfo = dataExtractor(data, "maimai.cn", x => x.mkString("\t"), Extractor.maimaiDataLink)

      case "jsdx" =>
        userInfo = dataExtractor(data, "maimai.cn", StringUtil.dataFormat, Extractor.maimaiDataLink)
    }

    try {

      for (index <- userInfo.indices) {

        PLogger.warn(index + "\tof\t" + userInfo.length)
        val x = userInfo(index)
        val task = new MaiMaiSubRunable(x._1, x._3, x._4)
        val future = es.submit(task)
        userMapListBuffer.+=((x._2, x._3, future.get()._2))

      }

      userMapListBuffer.foreach { list =>
        val map = list._3
        val ad = list._1
        val ua = list._2
        map.+=("ad" -> ad)
        map.+=("ua" -> ua)
        val maiMai = getMaiMainMainIndex(map)

        if(maiMai._1.phone != "Nodef")
          userInfoList.+=(maiMai._2)
      }

    } catch {

      case e:Exception => PLogger.warn("task error")

    } finally {

      FileUtil.writeToFile(savePath,userInfoList.distinct.toArray)
      PLogger.warn("写入文件完毕..................:" + userInfoList.distinct.toArray.length)
      es.shutdown()

    }

    sc.stop()

  }


  def dataExtractor(rdd: RDD[String], url: String, formatFunction:Array[String] => String, filterFunction: Array[String] => (String,String,String,String)): Array[ (String,String,String,String)] = {

    val result = rdd.map(_.split("\t"))
      .filter(_.length >= 7)
      .map(formatFunction)
      .map(_.split("\t"))
      .filter(_.length >= 7)
      .filter(x => x(6) != "NoDef")
      .filter(x => x(3).contains(url))
      .map(filterFunction)
      .filter(_._1 != "")
      .distinct()
      .collect()

    result

  }

  /**
    * 获取main_index 中的id号
    * @param map 解析好的数据map集合
    * @return  返回一个case类和数据库表对应, 格式化后用户信息字符串
    */
  def getMaiMainMainIndex(map: mutable.HashMap[String, String]): (MaiMai, String) = {

    var info = ""
    var maiMai: MaiMai = null
    var phone = "Nodef"
    val email = map.getOrElse("邮箱", "")
    val job = map.getOrElse("工作", "")
    val position = map.getOrElse("职位", "")
    val realName = map.getOrElse("姓名", "")
    val company = map.getOrElse("工作经历", "")
    val education = map.getOrElse("教育经历", "")
    var address = map.getOrElse("地址", "Nodef")
    val ua = map.getOrElse("ua", "Nodef")
    val ad = map.getOrElse("ad", "Nodef")

    if(map.contains("手机号") && !map.get("手机号").get.contains("好友级别可见")) {
      phone = map.get("手机号").get
    }

    val mainIndexId = -1

    if(address == "Nodef") {
      address = map.getOrElse("地区", "")
    }

    maiMai =  MaiMai(mainIndexId, phone, email, job, position, realName, company, education, address)
    info = ad + "\t" + ua + "\t" + realName + "\t" + phone +  "\t" + email +  "\t" +  position +  "\t" + job +  "\t" + address +  "\t" + company +  "\t" + education

    (maiMai, info)

  }

}
