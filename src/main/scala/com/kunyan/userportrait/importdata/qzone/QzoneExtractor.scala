package com.kunyan.userportrait.importdata.qzone

import com.kunyan.userportrait.util.StringUtil
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yangshuai on 2016/3/2.
  * 从cookie提取qq号
  */
object QzoneExtractor {

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
      .filter(_(3).contains("qq.com"))
      .map(extractCookie)
      .filter(x => x(2).contains("qzone_check=") || x(2).contains("o_cookie="))
      .map(x => (extractQQ(x), 1))
      .reduceByKey(_+_)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(args(1))

    sc.stop()
  }

  def countNumber(pair: (String, Iterable[String])): String = {

    val key = pair._1

    val map = mutable.Map[String, Int]()

    pair._2.foreach(qq => {
      val count = map.getOrElse(qq, 0)
      map.put(qq, count + 1)
    })

    var result = ""
    map.foreach(x => {
      result += x._1 + "->" + x._2 + ","
    })

    val keyArr = key.split("->")

    keyArr.mkString("\t") + "\t" + result
  }

  def extractQQ(arr: Array[String]): String = {

    val cookie = arr(2)

    var qqStr = ""

    if (cookie.contains("qzone_check")) {
      val arr = cookie.split("qzone_check=")
      if (arr.length > 1) {
        qqStr = arr(1).split("_")(0)
      }
    } else {
      val arr = cookie.split("o_cookie=")
      if (arr.length > 1) {
        qqStr = arr(1)
        if (qqStr.contains(";")) {
          qqStr = qqStr.split(";")(0)
        }
      }
    }

    arr(2) = qqStr

    DigestUtils.md5Hex(arr(0) + arr(1)) + "\t" + arr.mkString("\t")
  }

  def extractCookie(arr: Array[String]): Array[String] = {

    val ad = arr(1)
    val ua = arr(2)

    val cookie = StringUtil.decodeBase64(arr(4))

    Array[String](ad, ua, cookie)
  }

}
