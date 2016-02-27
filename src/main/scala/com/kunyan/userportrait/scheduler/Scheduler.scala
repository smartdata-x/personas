package com.kunyan.userportrait.scheduler

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/2/27.
  */
object Scheduler {

  var props = new Properties()
  props.put("metadata.broker.list", "222.73.34.92:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("USER PORTRAIT").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val source = args(0)

    val result = sc.textFile(source).map(x => x.split("\t"))
      .filter(_.length == 5)
      .filter(_(4) != "NoDef")
      .filter(_(3).contains("http://www.ele.me/"))

    result.foreach(x => {
      val json = toJson(x)
      val message = new KeyedMessage[String, String]("user", toJson(x))
      producer.send(message)
    })

    sc.stop()
  }




  def toJson(arr: Array[String]): String = {
    val json = "{\"ua\":%s, \"ad\":%s, \"url\":%d, \"cookie\":%d, \"platform\":%s\"}"
    json.format(arr(1), arr(2), arr(3), arr(4), "eleme")
  }

}
