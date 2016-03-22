package com.kunyan.userportrait.importdata.other

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yangshuai on 2016/3/2.
  */
object Aggregator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    val input = sc.textFile(args(0))
    input.map(_.split("\t"))
      .filter(_.length == 3)
      .map(combineKey)
      .groupByKey()
      .map(getResult)
      .saveAsTextFile(args(1))

    sc.stop()

  }

  def combineKey(arr: Array[String]): (String, String) = {

    val ad = arr(0)
    val ua = arr(1)

    (ad + "->" + ua, arr(2))

  }

  def getResult(pair: (String, Iterable[String])): String = {

    val keyArr = pair._1.split("->")
    val values = pair._2

    val mailMap = mutable.Map[String, Int]()
    val phoneMap = mutable.Map[String, Int]()
    val qqMap = mutable.Map[String, Int]()
    val weiboMap = mutable.Map[String, Int]()

    values.foreach(x => {
      x.split(",").foreach(arrowPair => {

        val arr = arrowPair.split("->")

        if (arr.length == 2) {

          var key = arr(0)
          val count = arr(1).toInt

          if (key.contains("%40"))
            key = key.replace("%40", "@")

          if (key.contains("@")) {
            mailMap.put(key, mailMap.getOrElse(key, 0) + count)
          } else if (key forall Character.isDigit) {
            if (key.length == 11 && key(0) == '1' && (key(1) == '3' || key(1) == '5' || key(1) == '8' || key(1) == '7')) {
              phoneMap.put(key, phoneMap.getOrElse(key, 0) + count)
            } else {
              qqMap.put(key, qqMap.getOrElse(key, 0) + count)
            }
          } else {
            weiboMap.put(key, weiboMap.getOrElse(key, 0) + count)
          }

        }
      })
    })

    keyArr(0) + "\t" + keyArr(1) + "\t" + convertMapToStr(mailMap) + "\t" + convertMapToStr(phoneMap) + "\t" + convertMapToStr(qqMap) + "\t" + convertMapToStr(weiboMap)
  }

  def convertMapToStr(map: mutable.Map[String, Int]): String = {

    var result = ""

    map.foreach(x => {
      result += x._1 + "->" + x._2 + ","
    })
    result
  }
}
