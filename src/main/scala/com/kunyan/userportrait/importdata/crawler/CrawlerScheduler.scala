package com.kunyan.userportrait.importdata.crawler

import com.kunyan.userportrait.config.SparkConfig
import com.kunyan.userportrait.importdata.crawler.request.MaimaiRequest
import com.kunyan.userportrait.importdata.extractor.Extractor
import com.kunyan.userportrait.util.StringUtil
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/12.
  */
object CrawlerScheduler {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc,Seconds(10))

  def flatMapFun(line: String): mutable.MutableList[String] = {
    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val res = StringUtil.parseJsonObject(line)
    if(res.nonEmpty){
      lineList.+=(res)
    }
    lineList
  }

  def main(args: Array[String]) {

      val rdd = sc.textFile("F:\\datatest\\telecom\\data\\2016-05-14\\12")
      val data = rdd.distinct()
      println("start extract user info:")
      val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("maimai.cn"))
        .map(Extractor.maimaiUserId)
        .filter(_._1 != "")
        .map(x => MaimaiRequest.sendMaimaiRequest(x._1, x._2, x._3))
        .foreach(println)

  }
}
