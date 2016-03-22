package com.kunyan.userportrait.importdata.eleme

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/16.
  */
object ElemeCookieExtractor {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ELEME COOKIE")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .map(_.split('\u0001'))
      .filter(_.length==4)
      .filter(x => x(2).contains("www.ele.me/profile/info") || x(2).contains("www.ele.me/profile/address"))
      .map(_.mkString("\t"))
      .saveAsTextFile(args(1))

    sc.stop()

  }
}
