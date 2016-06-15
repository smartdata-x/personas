package com.kunyan.userportrait.importdata.crawler.request

import java.net.{SocketException, SocketTimeoutException}
import java.util
import java.util.Random

import com.kunyan.userportrait.log.PLogger
import org.jsoup.Connection.Method
import org.jsoup.{HttpStatusException, Jsoup}

/**
  * Created by C.J.YOU on 2016/5/12.
  * 请求html 处理类
  */
class Request {

  /**
    * 获取代理ip
    * @return  代理ip的数组
    */
  def getIPs: Array[String] = {

    var ipPort: Array[String] = null

    try {
      val doc = Jsoup.connect("http://qsdrk.daili666api.com/ip/?tid=558465838696598&num=500&delay=5&foreign=none&ports=80,8080")
        .timeout(3000)
        .followRedirects(true)
        .execute()
      ipPort = doc.body().split("\r\n")

    } catch {

      case ex: HttpStatusException => changIP()
      case ex: SocketTimeoutException => PLogger.exception(ex)
      case ex: SocketException => PLogger.exception(ex)
      case ex: Exception => PLogger.warn("getIPs error")

    }

    ipPort

  }

  /**
    *更新代理ip值
    */
  def changIP(): Unit = {

    try {

      val ips = getIPs
      val ipPort = ips(new Random().nextInt(100)).split(":")
      val ip = ipPort(0)
      val port = ipPort(1)

      PLogger.warn(ip + "  " + port)

      System.getProperties.setProperty("http.proxyHost", ip)
      System.getProperties.setProperty("http.proxyPort", port)

    } catch {
      case e:Exception => PLogger.warn("changIP error")
    }

  }

  /**
    * @param cookieStr cookie
    * @return 返回键值对的cookie
    */
  private  def getCookies(cookieStr: String): util.HashMap[String, String] = {

    val cookieMap = new util.HashMap[String, String]()
    val cookieArr = cookieStr.split(";")

    for (line <- cookieArr) {

      val lineArr = line.split("=")

      if (lineArr.length > 1) {
        cookieMap.put(lineArr(0), lineArr(1))
      }
    }

    cookieMap

  }

  /**
    * 请求数据
    * @param ua user agent
    * @param url url
    * @param cookie cookie
    * @return 请求结果
    */
  def sendRequest(ua:String, url:String, cookie: String): String = {

    var res = ""

    try {
      val response = Jsoup.connect(url)
        .ignoreContentType(true)
        .userAgent(ua)
        .method(Method.GET)
        .timeout(50000)
        .cookies(getCookies(cookie))
        .followRedirects(true)
        .execute()
      res = response.body()

    } catch {

      case exception: SocketTimeoutException => changIP()
        PLogger.warn("sendRequest time out")
      case exception: HttpStatusException => PLogger.warn("HttpStatusException out")
      case exception: Exception => PLogger.warn("sendRequest time out")

    }

    res

  }

}
