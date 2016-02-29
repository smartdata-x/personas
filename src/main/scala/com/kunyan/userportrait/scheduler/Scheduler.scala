package com.kunyan.userportrait.scheduler

import java.util.Properties

import kafka.producer.{Producer, ProducerConfig}
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

    if (args.length != 2)
      println("lack of parameters")

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    val source = args(0)

    sc.textFile(source).map(x => x.split("\t"))
      .filter(_.length == 5)
      .filter(_(4) != "NoDef")
      .filter(_(3).contains("ele.me/"))
      .filter(!_(3).contains("favicon.ico"))
      .map(toJson)
      .saveAsTextFile(args(1))

    /*result.foreach(x => {
      val message = new KeyedMessage[String, String](args(1), toJson(x))
      producer.send(message)
    })*/

    sc.stop()
  }




  def toJson(arr: Array[String]): String = {
    val json = "{\"ua\":\"%s\", \"ad\":\"%s\", \"url\":\"%s\", \"cookie\":\"%s\", \"platform\":\"%s\"}"
    json.format(arr(1), arr(2), arr(3), arr(4), "eleme")
  }

}
