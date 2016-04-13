package com.kunyan.userportrait.temp.zrb
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yangshuai on 2016/3/30.
  */
object GameUser extends App{

  val sourceWeiboPhone= "/home/zhnagruibo/weibo_phone"
  val sourceEmail = "/home/zhangruibo/email.data"
  val sourceQQ = "/home/zhangruibo/qq.data"

  val conf = new SparkConf().setAppName("Game User Info")

  val sc = new SparkContext(conf)
  val lines3 = sc.textFile(sourceWeiboPhone)
  val lines4 = sc.textFile(sourceEmail)
  val lines5 = sc.textFile(sourceQQ)
  var map = Map[String, String]()

  val AdTotal = sc.textFile(args(0)).filter(_.length >= 2).map(x => infoGet(x))

  val AD = AdTotal.distinct()
  //  sc.textFile(test).filter(x=>(x.length>=2)).map(x=>infoGet(x)).map(x=>(x,1)).distinct().saveAsTextFile(target)
  val lt = List("http://poker.qq.com/","http://bbs.g.qq.com/forum.php?mod=forumdisplay&fid=119","http://pw.lianzhong.com","http://bbs.lianzhong.com/showforum-10030.aspx","http://www.pook.com/games/dzpk.html","http://:www.jj.cn")
  val row3_weiBo = {
    lines3.map(_.split("\t")).filter(_.length == 5).map(x => (x(1), x(3)))
  }
  val row4_email = {
    lines3.map(_.split("\t")).filter(_.length == 2).map(x => (x(0), x(1)))
  }
  val row5_qq = {
    lines3.map(_.split("\t")).filter(_.length == 2).map(x => (x(0), x(1)))
  }
  //  val x = getAd("lale3c4ac8e58a28feb3203d44l78344a9d2a").foreach(println)
  val AD_phone_email = AD.map(x=>getAd(x.toString)).saveAsTextFile(args(1))
  //  val AdCount = AdTotal.count().saveAsTextFile(tar)
  var res =  Map[String,String]()
  var qqcom = ""
  //以每一条标准AD作为key,先在_phone文件夹中寻找手机号，然后在去email文件中查找email，如果email没有再去QQ文件中查找QQ，并补全email，
  //其中lookup返回的是一个数组，QQ号也许多个
  def getAd(arr:(String)):Map[String,String] = {

    for(i <- arr){
      val phone = row3_weiBo.lookup(i.toString)
      if(phone.nonEmpty){
        val email = row4_email.lookup(i.toString)
        res += (i.toString -> phone.toString)
        if(email != ""){
          res += (i.toString -> email.toString)
        }else{
          val qq = row5_qq.lookup(i.toString)
          if(qq != "") {
            for (j <- qq) {
              qqcom += (j + "@qq.com")
            }
            res += (i.toString -> qqcom)
          }else{
            res += (i.toString -> "")
          }
        }
      }else{
        res += (i.toString -> "")
        val email = row4_email.lookup(i.toString)
        if(email != ""){
          res += (i.toString -> email.toString)
        }else{
          val qq = row5_qq.lookup(i.toString)
          if(qq != "") {
            for (j <- qq) {
              qqcom += (j + "@qq.com")
            }
            res += (i.toString -> qqcom)
          }
        }
      }
    }
    res
  }

  def infoGet(str:String):String= {
    val strArr = str.split("\u0001")
    var ad = ""
    var url = ""
    var bl = false
    for (arr <- strArr) {
      if (arr.trim().length == 40) {
        ad = arr.trim()
      }else if(arr.trim().startsWith("http://")){
        url = arr.trim()
        bl = isValue(url)
      }
    }
    if(bl){
      ad
    }else{
      ""
    }
  }

  def isValue(str:String):Boolean={
    var flag = false
    val lt = List("http://poker.qq.com/","http://bbs.g.qq.com/forum.php?mod=forumdisplay&fid=119","http://pw.lianzhong.com",
      "http://bbs.lianzhong.com/showforum-10030.aspx","http://www.pook.com/games/dzpk.html","http://:www.jj.cn")
    for(i<-lt){
      if(str.startsWith(i.toString)){
        flag = true
      }
    }
    flag
  }

}
