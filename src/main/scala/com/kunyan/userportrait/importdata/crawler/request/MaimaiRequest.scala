package com.kunyan.userportrait.importdata.crawler.request

import java.net.SocketTimeoutException

import com.kunyan.userportrait.importdata.crawler.parser.MaiMaiParser
import com.kunyan.userportrait.log.PLogger
import org.jsoup.HttpStatusException

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/5/16.
  * maimai.cn 网站抓取网页数据主类
  */
object MaiMaiRequest  extends  Request {

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
      case exception: SocketTimeoutException => changIP()
      case exception: Exception => PLogger.warn("requestForUserInfo exception")
    }

    res

  }

  /**
    * @param uid  从cookie中解析到的uid
    * @param ua 用户ua
    * @param cookie 用户cookie
    * @return 返回网页数据
    */
  def sendMaimaiRequest(uid: String, ua: String, cookie: String): (String, mutable.HashMap[String,String])  = {

    // Thread.sleep(1000)  // 使用流处理的话这个需要注释掉

    val url = "https://maimai.cn/contact/detail/" + uid
    val res = requestForUserInfo(url, ua, cookie)
    val info = MaiMaiParser.extractorInfo(res)

    info

  }

  def getContactListDetail(ua: String, cookie: String): mutable.HashSet[String] = {

    changIP()

    val uidHashSet = new mutable.HashSet[String]
    var res = ""
    var index = 0

    try {

      while (index >= 0 && index <= 300 ) {

        val url = "https://maimai.cn/contact/inapp_dist1_list?start=" + index + "&jsononly=1"

        if(cookie.nonEmpty) {

          res = sendRequest(ua,url,cookie)
          val ids = MaiMaiParser.extractorMaiMaiUid(res)

          if(ids.nonEmpty){
            uidHashSet.++=(ids)
          }
        }
        index += 15
      }
    } catch {
      case exception: HttpStatusException => PLogger.warn("contact list fetch over")
      case exception: SocketTimeoutException => changIP()
      case exception: Exception => PLogger.warn("sendRequest out")
    }

    uidHashSet

  }

}
