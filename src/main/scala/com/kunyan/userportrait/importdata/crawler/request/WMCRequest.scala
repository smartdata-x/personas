package com.kunyan.userportrait.importdata.crawler.request

import java.net.SocketTimeoutException
import java.util
import java.util.regex.Pattern

import com.kunyan.userportrait.log.PLogger
import org.json.JSONObject
import org.jsoup.Connection.Method
import org.jsoup.Jsoup

/**
  * Created by C.J.YOU on 2016/5/25.
  * 外卖超人的请求类
  */
object WMCRequest extends  Request {

  /**
    * 爬取waimaichaoren用户信息
    * @param arr 用于爬取用户数据的ua cookie
    * @return 用户数据
    */
  def crawlerWMCR(arr: Array[String]): String = {

    val ua = arr(2)
    val cookie = arr(6)
    var info = ""

    try {

      val doc = Jsoup.connect("http://waimaichaoren.com/account/center/address/")
        .userAgent(ua)
        .cookies(getCookieMap(cookie))
        .timeout(3000)
        .method(Method.GET)
        .execute()

      info = parseHtml(doc.body())


    } catch {

      case ex: SocketTimeoutException => changIP()
      case e:Exception => PLogger.warn("exception")

    }

    info
  }

  /**
    * 解析请求到的网页源数据
    * @param html 网页源数据
    * @return 用户信息
    */
  def parseHtml(html: String): String = {

    var info = "Nodef"
    var name = "Nodef"
    var phone = "Nodef"
    val email = "Nodef"
    var address = "Nodef"

    val pattern = Pattern.compile("userAddress.push.*\\}")
    val m = pattern.matcher(html)

    if (m.find()) {

      val infoArray = m.group().split("\\(")(1)
      val jo = new JSONObject(infoArray)
      name = jo.getString("customer_name")
      address = jo.getString("delivery_address")
      phone = jo.getString("customer_phone")
      info = name + "-->" + phone + "-->" + email + "-->" + address

    }

    info

  }
  /**
    * 将cookie字符串转成map
    * 根据cookie字符串获取cookie的map
    */
  def getCookieMap(cookie: String): util.HashMap[String, String] = {

    val cookieMap = new util.HashMap[String, String]()
    val cookieStr = cookie
    val cookieArr = cookieStr.split(";")

    for (line <- cookieArr) {

      val lineArr = line.split("=")

      if (lineArr.length > 1) {

        cookieMap.put(lineArr(0), lineArr(1))

      }
    }

    cookieMap

  }

}
