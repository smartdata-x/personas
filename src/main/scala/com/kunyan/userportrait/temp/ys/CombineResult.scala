package com.kunyan.userportrait.temp.ys

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yangshuai on 2016/3/30.
  */
object CombineResult {

  def main(args: Array[String]): Unit = {

    case class AdInfo(key: String, value: String)

    val conf = new SparkConf().setAppName("Game User Info")
    val sc = new SparkContext(conf)

    val times = sc.textFile("/user/portrait/result/gameuser")
      .map(_.split("\t")).filter(_.length == 2).map(x => (x(0), "time->" + x(1)))

    val phones = sc.textFile("/user/portrait/result/phone/final")
      .map(_.split("\t")).filter(_.length == 2).map(x => (x(0), "phone->" + x(1)))

    val emails = sc.textFile("/user/portrait/result/email/final")
      .map(_.split("\t")).filter(_.length == 2).map(x => (x(0), "email->" + x(1)))

    val qqmails = sc.textFile("/user/portrait/result/email/qq")
      .map(_.split("\t")).filter(_.length == 2).map(x => (x(0), "qq->" + x(1)))

    times.union(phones).union(emails).union(qqmails).groupByKey().map(combine).filter(_(1) != "null").filter(x => x(2) != "null" || x(3) != "null").repartition(1)
      .sortBy(_(1).toInt, ascending = false).map(_.mkString("\t")).saveAsTextFile("/user/portrait/result/gameuserresult")

  }

  def combine(pair: (String, Iterable[String])): Array[String] = {

    val ad = pair._1
    val values = pair._2

    var time = "null"
    var phones = "null"
    var emails = "null"
    var qqmails = "null"

    values.foreach(x =>{
      if (x.contains("time->")) {
        time = x.replace("time->", "")
      } else if (x.contains("phone->")) {
        phones = x.replace("phone->", "")
      } else if (x.contains("email->")) {
        emails = x.replace("email->", "")
      } else if (x.contains("qq->")) {
        qqmails = x.replace("qq->", "")
      }
    })

    if (emails == "null")
      emails = qqmails

    Array[String](ad, time, phones, emails)
  }

}
