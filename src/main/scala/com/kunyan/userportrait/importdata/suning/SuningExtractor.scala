package com.kunyan.userportrait.importdata.suning

import com.kunyan.userportrait.util.StringUtil
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yangshuai on 2016/3/2.
  * 从cookie提取电话号码
  */
object SuningExtractor {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("lack of parameters")
      return
    }

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    val source = args(0)

    val input = sc.textFile(source).map(x => x.split("\t"))
      .filter(_.length == 5)
      .filter(_(4) != "NoDef")
      .filter(_(3).contains("suning.com"))
      .map(extractCookie)
      .filter(x => x(2).contains("idsLoginUserIdLastTime="))

    input.cache()

    input.map(x => (extractPhone(x), 1))
      .filter(_._1 != "")
      .reduceByKey(_+_)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(args(1) + "_phone")

    input.map(x => (extractMail(x), 1))
      .filter(_._1 != "")
      .reduceByKey(_+_)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(args(1) + "_mail")

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

  def extractPhone(arr: Array[String]): String = {

    val cookie = arr(2)

    var phone = ""

    val cookieArr = cookie.split("idsLoginUserIdLastTime=")
    if (cookieArr.length > 1) {
      val value = cookieArr(1).split(";")(0)
      if (value forall Character.isDigit)
        phone = value
    }

    if (phone == "") {
      ""
    } else {
      arr(2) = phone
      DigestUtils.md5Hex(arr(0) + arr(1)) + "\t" + arr.mkString("\t")
    }
  }

  def extractMail(arr: Array[String]): String = {

    val cookie = arr(2)

    var mail = ""

    val cookieArr = cookie.split("idsLoginUserIdLastTime=")
    if (cookieArr.length > 1) {
      val value = cookieArr(1).split(";")(0).replace("%40", "@")
      if (value.contains("@"))
        mail = value
    }

    if (mail == "") {
      ""
    } else {
      arr(2) = mail
      DigestUtils.md5Hex(arr(0) + arr(1)) + "\t" + arr.mkString("\t")
    }
  }

  def extractCookie(arr: Array[String]): Array[String] = {

    val ad = arr(1)
    val ua = arr(2)

    val cookie = StringUtil.decodeBase64(arr(4))

    Array[String](ad, ua, cookie)
  }

}
