package com.kunyan.userportrait.scheduler

import java.util.Properties

import com.kunyan.userportrait.util.StringUtil
import kafka.producer.{Producer, ProducerConfig}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yangshuai on 2016/2/27.
  */
object Scheduler {

/*  var props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)*/

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("lack of parameters")
      return
    }

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    val source = args(0)

    sc.textFile(source).map(x => x.split("\t"))
      .filter(_.length == 5)
      .filter(_(4) != "NoDef")
      .filter(_(3).contains("suning.com"))
      .map(extractCookie)
      .filter(x => x._2.contains("idsLoginUserIdLastTime="))
      .map(extractPhone)
      .groupByKey()
      .map(countNumber)
      .saveAsTextFile(args(1))

    /*result.foreach(x => {
      val message = new KeyedMessage[String, String](args(1), toJson(x))
      producer.send(message)
    })*/

    sc.stop()
  }

  def countNumber(pair: (String, Iterable[String])): String = {

    val key = pair._1

    val map = mutable.Map[String, Int]()

    pair._2.foreach(x => {
      val count = map.getOrElse(x, 0)
      map.put(x, count + 1)
    })

    var result = ""
    map.foreach(x => {
      result += x._1 + "->" + x._2 + ","
    })

    val keyArr = key.split("->")

    keyArr.mkString("\t") + "\t" + result
  }

  def extractPhone(pair: (String, String)): (String, String) = {

    val key = pair._1
    val cookie = pair._2

    var phone = ""

    val arr = cookie.split("idsLoginUserIdLastTime=")
    if (arr.length > 1) {
      phone = arr(1).split(";")(0)
    }

    (key, phone)
  }

  def extractCookie(arr: Array[String]): (String, String) = {

    val ad = arr(1)
    val ua = arr(2)

    val cookie = StringUtil.decodeBase64(arr(4))

    (ad + "->" + ua, cookie)
  }


  def toJson(arr: Array[String]): String = {
    val json = "{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"%s\", \"cookie\":\"%s\", \"platform\":\"%s\"}"
    json.format(arr(1), arr(2), arr(3), arr(4), "eleme")
  }

}
