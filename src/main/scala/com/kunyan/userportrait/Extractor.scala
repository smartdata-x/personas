package com.kunyan.userportrait

import com.kunyan.userportrait.util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yangshuai on 2016/2/24.
  */
object Extractor {

  var sqlContext: SQLContext = null

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)

    if(args.length < 2){
      System.err.println("at least 2 parameters")
      System.exit(-1)
    }

    sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 5)
      .filter(x => x(4) != "NoDef")
      .filter(_(3).contains("weibo.com"))
      .map(extractCookie)
      .filter(x => x._2.contains("SUS=SID-") || x._2.contains("un=") || x._2.contains("cSaveState=") || x._2.contains("wb_feed_unfolded_"))
      .flatMap(getUserInfo)
      .groupByKey()
      .map(convertValueFormat)
      .saveAsTextFile(args(1))

    sc.stop()

  }

  def convertValueFormat(pair:(String, Iterable[String])): String = {

    val keyArr = pair._1.split("->")
    val values = pair._2
    val map = mutable.Map[String, Int]()
    values.foreach(x =>{
      val count = map.getOrElse(x, 0)
      map.put(x, count + 1)
    })

    var result = ""
    map.foreach(x => {
      result += x._1 + "->" + x._2 + ","
    })

    keyArr(0) + "\t" + keyArr(1) + "\t" + result
  }

  def extractCookie(arr: Array[String]): (String, String) = {

    val ad = arr(1)
    val ua = arr(2)
    val cookie = StringUtil.decodeBase64(arr(4))

    (ad + "->" + ua, cookie)
  }

  def getUserInfo(pair:(String, String)): ListBuffer[(String,String)]={

    val key = pair._1
    val cookie = pair._2

    var list = ListBuffer[(String, String)]()

    if (cookie.contains("SUS=SID-")) {
      val arr = cookie.split("SUS=SID-")
      if (arr.length > 1) {
        val info = arr(1).split("-")(0)
        list += key -> ("ss:" + info)
      }
    }

    if (cookie.contains("un=")) {
      val arr = cookie.split("un=")
      if (arr.length > 1) {
        if(arr(1).contains(";")){
          val info = arr(1).split(";")(0)
          list += key -> ("un:" + info)
        }else{
          val info = arr(1)
          list += key -> ("un:" + info)
        }
      }
    }

    if(cookie.contains("cSaveState=")){
      val arr = cookie.split("cSaveState=")
      if (arr.length > 1) {
        if(arr(1).contains(";")){
          val info = arr(1).split(";")(0)
          list += key -> ("css:" + info)
        }else{
          val info = arr(1)
          list += key -> ("css:" + info)
        }
      }
    }

    if(cookie.contains("wb_feed_unfolded_")){
      val arr = cookie.split("wb_feed_unfolded_")
      if (arr.length > 1) {
        val info = arr(1).split("=")(0)
        list += key -> ("wb:" + info)
      }
    }

    list
  }

}
