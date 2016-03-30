package com.kunyan.userportrait.importdata.other

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/19.
  */
object PokerCount {

  def main(args: Array[String]): Unit = {

    val set = Set[String](
      "http://poker.qq.com/",
      "http://bbs.g.qq.com/forum.php?mod=forumdisplay&fid=119",
      "http://pw.lianzhong.com",
      "http://bbs.lianzhong.com/showforum-10030.aspx",
      "http://www.pook.com/games/dzpk.html",
      "www.jj.cn")

    val sparkConf = new SparkConf().setAppName("USER PORTRAIT")
    val sc = new SparkContext(sparkConf)

    val setBr = sc.broadcast(set)

    sc.textFile(args(0)).map(_.split('\u0001')).filter(_.length==4)
      .map(arr => {
        var source = ""
        setBr.value.foreach(x => {
          if (arr(2).contains(x))
            source = x
        })
        (source, arr)
      })
      .filter(_._1 != "")
      .map(x => {
        (x._2(0) + "\t" + x._1, 1)
      })
      .reduceByKey(_+_)
      .map(pair => {
        pair._1 + "\t" + pair._2
      })
      .saveAsTextFile(args(1))
  }

}
