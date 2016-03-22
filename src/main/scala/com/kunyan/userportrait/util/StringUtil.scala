package com.kunyan.userportrait.util

import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Decoder

/**
  * Created by yangshuai on 2016/2/27.
  */
object StringUtil {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WEIBO")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/user/portrait/result/weibodata/mid").map(_.split("\t")).filter(_.length==4)
    input.map(x=>(getUserId(x(3)), x)).filter(_._1.nonEmpty).groupByKey().map(onlyOne).filter(_.nonEmpty).saveAsTextFile("/user/portrait/result/weibodata/step1")

  }

  def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  def getUserId(str: String): String = {

    val arr = decodeBase64(str).split("SUS=SID-")

    if (arr.length < 2)
      return ""

    val idArr = arr(1).split("-")

    if (idArr.length >= 2)
      return idArr(0)

    ""
  }

  def getResult(pair: (String, Array[String])): String = {

    val arr = pair._2

    if (arr == null)
      return ""

    val url = s"http://weibo.com/${pair._1}/profile"
    String.format("{\"ad\":\"%s\", \"ua\":\"%s\", \"url\":\"%s\", \"cookie\":\"%s\", \"platform\":\"weibo_step1\"}", arr(0), arr(1), url, arr(3))

  }

  def onlyOne(pair: (String, Iterable[Array[String]])): String = {

    val userId = pair._1
    val iterator = pair._2

    var result: Array[String] = null
    var timeStr = ""
    var timeStamp = 0l

    iterator.seq.foreach(arr => {
      val cookie = arr(3)
      val cookieArr = decodeBase64(cookie).split("SUS=SID-")
      if (cookieArr.length >= 2) {
        val timeArr = cookieArr(1).split("-")
        if (timeArr.length >= 2) {
          timeStr = timeArr(1)
          if (timeStr forall Character.isDigit) {
            if (timeStamp < timeStr.toLong) {
              timeStamp = timeStr.toLong
              result = arr
            }
          }
        }
      }
    })

    getResult((userId, result))
  }

}
