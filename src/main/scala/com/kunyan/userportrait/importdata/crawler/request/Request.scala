package com.kunyan.userportrait.importdata.crawler.request

import java.net.{SocketException, SocketTimeoutException}
import java.util
import java.util.Random

import org.jsoup.Connection.Method
import org.jsoup.{HttpStatusException, Jsoup}

/**
  * Created by C.J.YOU on 2016/5/12.
  * 请求html 处理类
  */
class Request {
  /**
    * @return  获取代理ip
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
      case ex: SocketTimeoutException => println(ex)
      case ex: SocketException => println(ex)
      case ex: Exception => println("getIPs error")

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

      println(ip + "  " + port)

      System.getProperties.setProperty("http.proxyHost", ip)
      System.getProperties.setProperty("http.proxyPort", port)

    } catch {
      case e:Exception => println("changIP error")
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
        println("sendRequest time out")
      case exception: HttpStatusException => println("HttpStatusException out")
      case exception: Exception => println("sendRequest time out")
    }

    res

  }

}