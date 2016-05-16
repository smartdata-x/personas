package com.kunyan.userportrait.importdata.scheduler

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.db.{DBOperation, Table}
import com.kunyan.userportrait.importdata.extractor.Extractor
import com.kunyan.userportrait.util.StringUtil
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/10.
  * 策略一 （实时）
  */
object TimeScheduler {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer", SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max", SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc, Seconds(2))

  var distinctPhone = new HashSet[String]
  var distinctQQ = new HashSet[String]
  var distinctWeibo = new HashSet[String]

  def flatMapFun(line: String): mutable.MutableList[String] = {

    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val res = StringUtil.parseJsonObject(line)

    if(res.nonEmpty) {
      lineList.+=(res)
    }

    lineList

  }

  def main(args: Array[String]): Unit = {

    val Array(brokers, topics, zkhosts) = args

    val lineData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
       kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "newpersonas","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
       topics.split(",").toSet
    )

    val result = lineData.flatMap(x => flatMapFun(x._2))
    result.foreachRDD { rdd =>
      try {
        println("init..................")
        distinctPhone = distinctPhone.empty
        distinctQQ = distinctQQ.empty
        distinctWeibo  =  distinctWeibo.empty
        Table.resetInstance()

        val data = rdd.distinct()
        println("start extract user info:")

        val userInfo  = data.map(_.split("\t"))
          .filter(_.length == 8)
          .filter(x => x(6) != "NoDef")
          .filter(x => x(3).contains("weibo.com") || x(3).contains("suning.com") || x(3).contains("qq.com") ||x(3).contains("youtx.com")
            || x(3).contains("dianping.com") || x(3).contains("4008823823.com.cn")|| x(3).contains("189.cn") || x(3).contains("jiayuan.com")
            || x(3).contains("10jqka.com.cn") || x(3).contains("email.163.com") || x(3).contains("baixing.com") || x(3).contains("weidian.com")
            || x(3).contains("item.yhd.com")|| x(3).contains("order.jd.com")|| x(3).contains("renren.com")|| x(3).contains("eastmoney.com")
            || x(3).contains("kejiqi.com")|| x(3).contains("music.migu.cn") || x(3).contains("elong.com") || x(3).contains("1zhe.com")
            || x(3).contains("esf.fangdd.com") || x(3).contains("yougou.com"))
          .map(Extractor.extractorUserInfo)
          .filter(filterUserInfo)
          .distinct()

        val updateUserInfo = userInfo.map(divideInsertMap).groupByKey()
        println("divideInsertMap overed...........................")
        val item0 = updateUserInfo.lookup(0)

        if(item0.nonEmpty) {

          val insertUserInfo = item0.head.filter(filterUserInfo)
          println("insertIterator size:"+insertUserInfo.size)
          println("INSERTING.........................")
          DBOperation.batchInsert(Array("phone", "qq", "weibo"),insertUserInfo)
          println("INSERTED.........................")

        }
        val item1 = updateUserInfo.lookup(1)
        val item2 = updateUserInfo.lookup(2)
        val item3 = updateUserInfo.lookup(3)
        println("UPDATING...........................")

        if(item1.nonEmpty) {

          val phoneUpdateIterator = item1.head
          println("phoneUpdateIterator size:"+phoneUpdateIterator.size)
          DBOperation.batchUpdate(Array("qq", "weibo"),phoneUpdateIterator)

        }
        if(item2.nonEmpty) {

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

      }
      println("end")

    }
    ssc.start()
    ssc.awaitTermination()

  }
  def filterUserInfo(x:(String, String, String)): Boolean = {
    if(x._1.nonEmpty || x._2.nonEmpty || x._3.nonEmpty) {
      true
    } else {
      false
    }
  }

  /**
    * @param item  数据的电信源数据
    * @return  返回分组后的数据，用于区分是插入还是更新
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

    val isDistinctPhone = distinctPhone.contains(phone)
    val isDistinctQQ = distinctQQ.contains(qq)
    val isDistinctWeibo = distinctWeibo.contains(weibo)

    val phoneDataFrame = Table.isExist("phone",phone,DBOperation.connection)

    if(phoneDataFrame._1 != -1) {

      val id = phoneDataFrame._1

      if (qq == "Nodef"){
        qq = phoneDataFrame._3
      }

      if (weibo == "Nodef"){
        weibo = phoneDataFrame._4
      }

      (1,(qq, weibo, id.toString))

    } else {

      val qqDataFrame = Table.isExist("qq",qq,DBOperation.connection)

      if(qqDataFrame._1 != -1) {

        val id = qqDataFrame._1

        if (phone == "Nodef") {
          phone = qqDataFrame._2
        }

        if (weibo == "Nodef") {
          weibo = qqDataFrame._4
        }

        (2,(phone, weibo, id.toString))

      } else {
        val weiboDataFrame = Table.isExist("weibo",weibo,DBOperation.connection)

        if(weiboDataFrame._1 != -1) {

          val id = weiboDataFrame._1

          if (phone == "Nodef") {
            phone = weiboDataFrame._2
          }

          if (qq == "Nodef") {
            qq = weiboDataFrame._3
          }

          (3,(phone, qq, id.toString))

        } else {

          if(!isDistinctPhone && !isDistinctQQ && !isDistinctWeibo){

            if(phone != "Nodef") {
              distinctPhone = distinctPhone.+(phone)
            }

            if(qq != "Nodef") {
              distinctQQ = distinctQQ.+(qq)
            }

            if(weibo != "Nodef") {
              distinctWeibo = distinctWeibo.+(weibo)
            }

            (0,(item._1, item._2, item._3))

          } else {
            (0,("","",""))
          }
        }
      }
    }
  }
}
