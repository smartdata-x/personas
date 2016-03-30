package com.kunyan.userportrait.importdata.other

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangshuai on 2016/3/19.
  */
object TargetWebsites {

  def main(args: Array[String]): Unit = {

    val set = Set[String](
      "http://www.bpex.com.cn/",
      "http://www.hbot.cn/",
      "https://portal.jsdyyt.com/register/signup",
      "https://me.91jin.com/yykh.html?s=hj",
      "https://me.91shihua.com/qilu/shipan.html?entercode=gxsh_gw_pcweb_dh_spkh",
      "http://www.dby059.com/login_real01.html",
      "http://www.shneapme.com/2-chanpinjifuwu/536.html",
      "http://tjsbot.com/kaihu.asp",
      "http://open.eboce.com/kaihus/",
      "https://selfadd.hxnme.com.cn:17785/SelfOpenAccount/first.jsp?memberNo=0",
      "http://www.huaxiayin.cn/zhenshi/?119_1.html",
      "http://open.qdpme.com/accountOL/",
      "http://www.zgzadz.com/")

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
