package com.kunyan.userportrait.importdata.crawler.request

import java.net.SocketTimeoutException

import com.kunyan.userportrait.importdata.crawler.parser.MaimaiParser


/**
  * Created by C.J.YOU on 2016/5/16.
  * maimai.cn 网站抓取网页数据主类
  */
object MaimaiRequest  extends  Request {

  /**
    * @param url 方法的url
    * @param ua  用户ua
    * @param cookie 用户的cookie
    * @return 返回网页数据
    */
  def requestForUserInfo(url: String, ua: String, cookie: String): String = {
    var res = ""
    try {
      if(cookie.nonEmpty) {
        res = sendRequest(ua,url,cookie)
      }
    } catch {
      case exception: SocketTimeoutException => println("requestForUserInfo time out")
    }
    res
  }

  /**
    * @param uid  从cookie中解析到的uid
    * @param ua 用户ua
    * @param cookie 用户cookie
    * @return 返回网页数据
    */
  def sendMaimaiRequest(uid: String, ua: String, cookie: String): String  = {

    println("maimaiUid :" + uid)
    val url = "https://maimai.cn/contact/detail/" + uid
    val res = requestForUserInfo(url, ua, cookie)
    val info = MaimaiParser.extractorInfo(res)

    info

  }

}
