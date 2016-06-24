package com.kunyan.userportrait.importdata.crawler

import java.util.concurrent.Future

import com.kunyan.userportrait.config.{PlatformConfig, SparkConfig}
import com.kunyan.userportrait.db.{DBOperation, Table}
import com.kunyan.userportrait.db.Table.{MaiMai, WaiMai}
import com.kunyan.userportrait.importdata.crawler.request.{KFCRequest, MaiMaiRequest, WMCRequest}
import com.kunyan.userportrait.importdata.extractor.Extractor
import com.kunyan.userportrait.log.PLogger
import com.kunyan.userportrait.util.{FileUtil, StringUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/5/12.
  * maimai 平台数据爬取调度类
  */
object CrawlerMaiMaiTimeScheduler {

  val sparkConf = new SparkConf()
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc, Seconds(5))

  val userMaiMaiInfoList = new ListBuffer[String]()
  val userWaiMaiInfoList = new ListBuffer[String]()

  val set = new mutable.HashSet[Future[mutable.HashSet[(String,mutable.HashMap[String,String])]]]()
  val setTwo = new mutable.HashSet[(String,mutable.HashMap[String,String])]()

  val userMaiMaiList = new ListBuffer[(MaiMai,String)]()
  val userWaiMaiList = new ListBuffer[(WaiMai,String)]()

  def flatMapFun(line: String): mutable.MutableList[String] = {

    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val res = StringUtil.parseJsonObject(line)

    if(res.nonEmpty) {
      lineList.+=(res)
    }

    lineList

  }
  def main(args: Array[String]) {


    val Array(brokers, topics, zkhosts, maidir, kfcdir) = args

    val lineData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "crawlernewpersonas","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics.split(",").toSet
    )

    val result = lineData.flatMap(x => flatMapFun(x._2))
    result.foreachRDD { rdd =>

      userMaiMaiInfoList.clear()
      userMaiMaiList.clear()
      setTwo.clear()
      userWaiMaiList.clear()
      userWaiMaiInfoList.clear()

      val data = rdd.distinct()

      try {
        PLogger.warn("start maimai extract user info:")
        val userInfo = data.map(_.split("\t"))
          .filter(_.length == 8)
          .filter(x => x(6) != "NoDef")
          .filter(x => x(3).contains("maimai.cn"))
          .map(Extractor.maimaiUserId)
          .filter(_._1 != "")
          .distinct()
          .map { x =>
            val contactList = MaiMaiRequest.getContactListDetail(x._2, x._3)
            contactList.+=(x._1)
            (contactList, x._2, x._3)
          }.foreachPartition { x =>
          x.foreach { list =>
            list._1.foreach(x => {
              val info = MaiMaiRequest.sendMaimaiRequest(x, list._2, list._3)
              setTwo.+=(info)
            })
          }
        }
        /**
          * kfc 数据提取
          */
        PLogger.warn("start kfc extract user info:")
        val kfcUserInfo = data.map(_.split("\t"))
          .filter(_.length == 8)
          .filter(x => x(6) != "NoDef")
          .filter(x => x(3).contains("4008823823.com.cn"))
          .map(KFCRequest.crawlerKfc)
          .distinct()
          .filter(!_.contains("*"))
          .foreach { x =>
            val o2o = getWaiMaiMainIndex(x)
            userWaiMaiList.+=((o2o._1,o2o._2))
            userWaiMaiInfoList.+(o2o._2)
          }

        /**
          * waimaichaoren 数据提取
          */
        PLogger.warn("start waimaichaoren extract user info:")
        val wmcrUserInfo = data.map(_.split("\t"))
          .filter(_.length == 8)
          .filter(x => x(6) != "NoDef")
          .filter(x => x(3).contains("waimaichaoren.com"))
          .map(WMCRequest.crawlerWMCR)
          .distinct()
          .filter(!_.contains("*"))
          .foreach { x =>
            val o2o = getWaiMaiMainIndex(x)
            userWaiMaiList.+=((o2o._1,o2o._2))
            userWaiMaiInfoList.+(o2o._2)
          }

      } catch {
        case e: Exception => PLogger.warn(e.getMessage)
      }

      PLogger.warn("set2.size：" + setTwo.size)
      try {

        PLogger.warn("解析完毕，开始写入数据库..................")
        setTwo.foreach { x =>
          val maiMai = getMaiMainMainIndex(x._2)
          userMaiMaiList.+=((maiMai._1,maiMai._2))
          userMaiMaiInfoList.+=(maiMai._2)
        }
        PLogger.warn("MaiMai解析完毕，开始写入数据库..................")
        val exist = userMaiMaiList.filter(x => x._1.mainIndex != -1)
        val noExist = userMaiMaiList.filter(x => x._1.mainIndex == -1).filter(x => x._1.phone != "Nodef")

        val updateMain = noExist.map(x =>(x._1.phone,"",""))
        DBOperation.batchInsert(Array("phone","qq","weibo"),updateMain)
        DBOperation.maiMaiInsert(exist)

        val newExist = noExist.map(x => {
          x._1.mainIndex = Table.isExist("phone",x._1.phone,DBOperation.connection)._1
          x
        }).filter(_._1.mainIndex != -1)
        DBOperation.maiMaiInsert(newExist)

        PLogger.warn("MaiMai写入数据库完毕..................:" + noExist.size)

        FileUtil.writeToFile(maidir,userMaiMaiInfoList.distinct.toArray)
        PLogger.warn("MaiMai写入文件完毕..................")

      } catch {
        case e: Exception => PLogger.warn(e.getMessage)
      } finally {
      }

      try {

        PLogger.warn("O2O解析完毕，开始写入数据库..................")
        val exist = userWaiMaiList.filter(x => x._1.mainIndex != -1)
        val noExist = userWaiMaiList.filter(x => x._1.mainIndex == -1).filter(x => x._1.phone != "Nodef")
        val updateMain = noExist.map(x =>(x._1.phone,"",""))
        DBOperation.batchInsert(Array("phone","qq","weibo"),updateMain)
        DBOperation.waiMaiInsert(exist,PlatformConfig.KFC)
        val newExist = noExist.map(x => {
          x._1.mainIndex = Table.isExist("phone",x._1.phone,DBOperation.connection)._1
          x
        }).filter(_._1.mainIndex != -1)

        DBOperation.waiMaiInsert(newExist,PlatformConfig.KFC)
        PLogger.warn("O2O写入数据库完毕..................:" + noExist.size)
        FileUtil.writeToFile(kfcdir,userWaiMaiInfoList.distinct.toArray)
        PLogger.warn("O2O写入文件完毕..................")

      } catch {
        case e: Exception => PLogger.warn(e.getMessage)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取main_index 中的id号
    * @param map 解析好的数据map集合
    * @return  返回一个case类和数据库表对应, 格式化后用户信息字符串
    */
  def getMaiMainMainIndex(map: mutable.HashMap[String,String]): (MaiMai, String) = {

    var info = ""
    var maiMai:MaiMai = null
    val phone = if(map.contains("手机号") && !map.get("手机号").get.contains("提升好友级别可见")) map.get("手机号").get else "Nodef"
    val mainIndex = Table.isExist("phone",phone,DBOperation.connection)
    val mainIndexId = mainIndex._1
    val email = if(map.contains("邮箱")) map.get("邮箱").get else ""
    val job = if(map.contains("工作")) map.get("工作").get else ""
    val position = if(map.contains("职位")) map.get("职位").get else ""
    val realName = if(map.contains("姓名")) map.get("姓名").get else ""
    val company = if(map.contains("工作经历")) map.get("工作经历").get else ""
    val education = if(map.contains("教育经历")) map.get("教育经历").get else ""
    var address = if(map.contains("地址")) map.get("地址").get else "Nodef"

    if(address == "Nodef") {
      address = if(map.contains("地区")) map.get("地区").get else ""
    }
    val maimai = MaiMai(mainIndexId,phone,email,job,position,realName,company,education,address)
    maiMai = maimai
    info = realName + "\t" + phone +  "\t" + email +  "\t" +  position +  "\t" + job +  "\t" + address +  "\t" + company +  "\t" + education

    (maiMai,info)

  }

  /**
  * 获取main_index 中的id号
  * @param info 解析好的用户信息字符串
  * @return  返回一个case类和数据库表对应, 格式化后用户信息字符串
  */
  def getWaiMaiMainIndex(info: String): (WaiMai, String) = {

    var o2o: WaiMai = null
    try {
      val arr = info.split("-->")
      val name = arr(0)
      val phone = if (arr(1) != "Nodef") arr(1) else "Nodef"
      val email = arr(2)
      val address = arr(3)
      val mainIndex = Table.isExist("phone", phone, DBOperation.connection)
      val mainIndexId = mainIndex._1
      o2o = WaiMai(mainIndexId, phone, email, name, address)
    } catch {
      case e:Exception =>
    }

    (o2o,o2o.toString)

  }
}
