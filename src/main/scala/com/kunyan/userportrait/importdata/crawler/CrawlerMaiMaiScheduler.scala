package com.kunyan.userportrait.importdata.crawler

import java.util.concurrent.{ExecutorCompletionService, Executors, Future}

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.db.Table.MaiMai
import com.kunyan.userportrait.db.{DBOperation, Table}
import com.kunyan.userportrait.importdata.crawler.request.MaiMaiRequest
import com.kunyan.userportrait.importdata.crawler.task.MaiMaiSubTask
import com.kunyan.userportrait.importdata.extractor.Extractor
import com.kunyan.userportrait.log.PLogger
import com.kunyan.userportrait.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/5/12.
  * maimai 平台数据爬取调度类
  */
object CrawlerMaiMaiScheduler {

  val sparkConf = new SparkConf()
    .setAppName("USER")
    .set("spark.serializer", SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max", SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)
  val userInfoList = new ListBuffer[String]()
  val userMaiMaiList = new ListBuffer[(MaiMai, String)]()
  val userMapListBuffer = new ListBuffer[mutable.HashMap[String, String]]()
  val userKeyMap = new mutable.HashMap[String, mutable.HashMap[String, String]]()
  val set = new mutable.HashSet[Future[mutable.HashSet[(String, mutable.HashMap[String, String])]]]()
  val subSet = new mutable.HashSet[Future[(String, mutable.HashMap[String, String])]]()

  def main(args: Array[String]) {

    val thread = 12
    val es = Executors.newFixedThreadPool(thread)
    val compService  = new ExecutorCompletionService[mutable.HashSet[(String, mutable.HashMap[String, String])]](es)
    val rdd = sc.textFile(args(0))
    val data = rdd.distinct()
    PLogger.warn("start extract user info:")

    val userInfo  = data.map(_.split("\t"))
      .filter(_.length == 8)
      .filter(x => x(6) != "NoDef")
      .filter(x => x(3).contains("maimai.cn"))
      .map(Extractor.maimaiUserId)
      .filter(_._1 != "")
      .distinct()
      .collect()

    try {

      for (index <- userInfo.indices) {

        PLogger.warn(index + "\tof\t" + userInfo.length)
        val x = userInfo(index)
        val contactList = MaiMaiRequest.getContactListDetail(x._2, x._3)
        contactList.+=(x._1)
        val task = new MaiMaiSubTask(contactList, x._2, x._3)
        compService.submit(task)

      }
      es.shutdown()

      for (index <- userInfo.indices) {

        PLogger.warn(index + "\tof\t" + userInfo.length)
        val future = compService.take()
        future.get().foreach { x =>
          userMapListBuffer.+=(x._2)
        }
      }

      userMapListBuffer.foreach { map =>
        var phone = "Nodef"

        if(map.contains("手机号") && !map.get("手机号").get.contains("好友级别可见")) {
          phone = map.get("手机号").get
        }

        if(!userKeyMap.contains(phone))
          userKeyMap.put(phone, map)
      }

      PLogger.warn("phone 去重完毕:"+ userKeyMap.size)

      val noUseMap = userKeyMap.filter(x => Table.isExistMaiMai("phone", x._1, DBOperation.connection)._1 == -1)

      PLogger.warn("phone 有效数量:"+ noUseMap.size)

      noUseMap.foreach { user =>
          val maiMai = getMaiMainMainIndex(user._2)
          userMaiMaiList.+=((maiMai._1, maiMai._2))
          userInfoList.+=(maiMai._2)
      }

      PLogger.warn("解析完毕，开始写入数据库..................")
      val exist = userMaiMaiList.filter(x => x._1.mainIndex != -1)
      val noExist = userMaiMaiList.filter(x => x._1.mainIndex == -1).filter(x => x._1.phone != "Nodef")

      val updateMain = noExist.map(x =>(x._1.phone, "", ""))
      DBOperation.batchInsert(Array("phone", "qq", "weibo"), updateMain)
      DBOperation.maiMaiInsert(exist)
      PLogger.warn("写入数据库完毕..................:" + exist.size)

      val newExist = noExist.map(x => {
        x._1.mainIndex = Table.isExist("phone", x._1.phone, DBOperation.connection)._1
        x
      }).filter(_._1.mainIndex != -1)

      DBOperation.maiMaiInsert(newExist)
      PLogger.warn("写入数据库完毕..................:" + newExist.size)

    } catch {

      case e:Exception => PLogger.warn("task error")

    } finally {

      FileUtil.writeToFile(args(1), userInfoList.distinct.toArray)
      PLogger.warn("写入文件完毕..................")
      DBOperation.connection.close()
      es.shutdown()

    }

    sc.stop()

  }

  /**
    * 获取main_index 中的id号
    * @param map 解析好的数据map集合
    * @return  返回一个case类和数据库表对应, 格式化后用户信息字符串
    */
  def getMaiMainMainIndex(map: mutable.HashMap[String,String]): (MaiMai, String) = {

    var info = ""
    var maiMai:MaiMai = null
    var phone = "Nodef"
    val email = map.getOrElse("邮箱", "")
    val job = map.getOrElse("工作", "")
    val position = map.getOrElse("职位", "")
    val realName = map.getOrElse("姓名", "")
    val company = map.getOrElse("工作经历", "")
    val education = map.getOrElse("教育经历", "")
    var address = map.getOrElse("地址", "Nodef")

    if(map.contains("手机号") && !map.get("手机号").get.contains("好友级别可见")) {
      phone = map.get("手机号").get
    }

    val mainIndex = Table.isExist("phone", phone, DBOperation.connection)
    val mainIndexId = mainIndex._1

    if(address == "Nodef") {
      address = map.getOrElse("地区", "")
    }

    maiMai =  MaiMai(mainIndexId, phone, email, job, position, realName, company, education, address)
    info = realName + "\t" + phone +  "\t" + email +  "\t" +  position +  "\t" + job +  "\t" + address +  "\t" + company +  "\t" + education

    (maiMai, info)

  }
}
