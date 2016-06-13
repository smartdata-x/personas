package com.kunyan.userportrait.importdata.crawler.request

import java.net.SocketTimeoutException

import com.kunyan.userportrait.importdata.crawler.parser.MeiTuanParser
import com.kunyan.userportrait.log.PLogger

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/13.
  */
object MeiTuanRequest extends Request {

  def requestForUserInfo(url:String,ua:String,cookie:String): String = {
    var res = ""
    try {
      if(cookie.nonEmpty){
        res = sendRequest(ua,url,cookie)
      }
    } catch {
      case exception: SocketTimeoutException => println("requestForUserInfo time out")
      case exception: Exception => PLogger.warn("requestForUserInfo exception")
    }
    res
  }

  def sendMeiTuanRequest(ua: String, cookie: String): (String, mutable.HashMap[String,String])   = {
    val url = "http://www.meituan.com/account/setaddress"
    val res = requestForUserInfo(url, ua, cookie)
    val parser = MeiTuanParser.extractorInfo(res)
    parser
  }

}
