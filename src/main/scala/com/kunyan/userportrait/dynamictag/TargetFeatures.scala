package com.ml.TargetmanClassification

import org.apache.spark.storage.StorageLevel._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/4/15.
  * target:统计每个AD,或IP的访问网页的行为（奢侈品网站的访问来判定是否有钱；股票网站的访问来判定是否玩股票）
  * 统计指标用于做分类的features
  */
object TargetFeatures {

  def getUrlRemoveHead(url:String):String= {
    var part = url
    try {
      if (url.contains("http://")) {
        part = url.replace("http://", "").split("/").filter(x => x.size > 0)(0)
      } else if (url.contains("https://")) {
        part = url.replace("https://", "").split("/").filter(x => x.size > 0)(0)
      } else {
        part = url.split("/").filter(x => x.size > 0)(0)
      }
    } catch {
      case e:Exception =>
    }
    part
  }

  def getUrlBody(url:String):String= {
    val host = getUrlRemoveHead(url)
    var part = host
    try{
      if (host.startsWith("www.")){
        part = host.replace("www.","")
      } else if (!host.contains("www")){
        part = host
      }
    } catch {
      case e:Exception =>
    }
    part
  }

  def getUrlKey(url:String):String={
    var part = url
    if (url.contains("http://www")){
      part = url.substring(11,url.length)
    } else if (url.contains("https://www")){
      part = url.substring(12,url.length)
    } else {
      part = url
    }
    part
  }

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("TargetFeatures")//.setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * 1. input and pre-process data
      */
    //1.1 input total data from telecom
    val data = sc.textFile(args(0))
      .map(_.split("\t")).filter(x => x.size == 6)
      .map(x =>(x(1),x(2),getUrlBody(x(3)),getUrlBody(x(4))))
      .filter(x => !x._2.contains("spider") && !x._2.contains("Spider"))
      .filter(x => x._3 != "qq.com" && !x._3.contains("mail") && x._3 !="ip.com" && x._3 != "baidu.com" && x._3 != "so.com" && x._3 != "ti.com.cn" && x._3 != "le.com" && x._3 != "sina.com.cn" )
    data.persist(MEMORY_AND_DISK)

    //1.2 input target url (including both original url and extra url)
    val totalUrl = sc.textFile("args(1)")
      .map(x => x.trim)
      .map(x => getUrlKey(x))
      .distinct()
      .filter(x => x.length > 0)
      .collect.mkString(",", ",", ",")


    /**
      * 2. exploring the target url
      */

    //2.1 label the target url as 1 and cache the outputted records
    val labelLineRddUr = data.filter(x => totalUrl.contains(x._3)).map(x=>{
      val url = x._3
      val domain = url
      (x._1 +"\t"+x._2+ "\t" + domain, 1)
    }).reduceByKey(_ + _).sortBy(x=>x._2,false)

    labelLineRddUr.persist(MEMORY_AND_DISK)

    //2.2 calculate how many times each Ad visits the target url and how many distinct target url each Ad visits
    val visitNumUr = labelLineRddUr.map(x => (x._1.split("\t")(0), x._2)).reduceByKey(_ + _)
    val urlNumUr = labelLineRddUr.map(_._1.split("\t")).map(x=>(x(0),1)).reduceByKey(_+_)


    /**
      * 3. exploring the target reference
      */
    //3.1 label the target reference as 1 and cache the outputted records
    val labelLineRddRe = data.filter(x => totalUrl.contains(x._4)).map(x=>{
      val url = x._4
      val domain = url
      (x._1 + "\t" + domain, 1)
    }).reduceByKey(_ + _)

    labelLineRddRe.persist(MEMORY_AND_DISK)

    //3.2 calculate how many times each Ad visits the target url and how many distinct target url each Ad visits
    val visitNumRe = labelLineRddRe.map(x => (x._1.split("\t")(0), x._2)).reduceByKey(_ + _)
    val urlNumRe = labelLineRddRe.map(_._1.split("\t")).map(x=>(x(0),1)).reduceByKey(_+_)


    /**
      * 4. join the results
      */
    val finalTableUr = visitNumUr.join(urlNumUr)
    val finalTableRe = visitNumRe.join(urlNumRe)
    val finalTable = finalTableUr.join(finalTableRe).sortBy(x=>x._2._1._2,false)


    /**
      *  5.save the total result
      */
    finalTable.coalesce(1).saveAsTextFile("args(2)")

    data.unpersist()
    labelLineRddUr.unpersist()
    labelLineRddRe.unpersist()

    sc.stop()
  }
}
