package com.kunyan.userportrait.temp.ys

import com.kunyan.userportrait.config.SparkConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yangshuai on 2016/3/30.
  */
object GameUserInfo {

  val URLS = List[String](
    "http://poker.qq.com/",
    "http://bbs.g.qq.com/forum.php?mod=forumdisplay&fid=119",
    "http://pw.lianzhong.com",
    "http://bbs.lianzhong.com/showforum-10030.aspx",
    "http://www.pook.com/games/dzpk.html",
    "jj.cn")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Game User Info")
      .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
      .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    val sc = new SparkContext(conf)

    val urlsBr = sc.broadcast(URLS)

    sc.textFile("/user/portrait/data/lcm20160313")
      .map(_.split('\u0001')).filter(_.length == 4)
      .filter(x => validUrl(x(2), urlsBr))
      .map(x => (x(0), 1)).reduceByKey(_+_)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile("/user/portrait/result/gameuser")

    sc.stop()
  }

  /**
    * 目标URL
    */
  def validUrl(urlRaw: String, urlList: Broadcast[List[String]]): Boolean = {

    val url = urlRaw.split("refer")(0)

    urlList.value.foreach(x => {
      if (url.contains(x))
        return true
    })

    false
  }
}
