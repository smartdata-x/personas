package com.kunyan.userportrait.importdata.crawler.request

import java.io.IOException
import java.net.{ConnectException, SocketException, SocketTimeoutException}
import java.util
import java.util.regex.Pattern

import com.kunyan.userportrait.log.PLogger
import org.jsoup.Connection.Method
import org.jsoup.{HttpStatusException, Jsoup}

/**
 * Created by lcm on 2016/5/20.
 * 用于爬取KFC数据
 */
object KFCRequest extends  Request {

  /**
   * 爬取KFC用户信息
    * 注：用户信息很多为带*。如果您选择不设置密码，您送餐信息的主要内容会以*号遮蔽，如：虹桥路2号，会显示为“虹﹡••••••﹡2号”。该显示信息可能不受保护，建议您设置密码。
   * @param arr 用于爬取用户数据的ua cookie
   * @return 用户数据
   */
  def crawlerKfc(arr: Array[String]): String = {

    val ua = arr(2)
    val cookie = arr(6)
    var info = ""

    try {

      val doc = Jsoup.connect("http://www.4008823823.com.cn/kfcios/customer.action")
        .userAgent(ua)
        .cookies(getCookieMap(cookie))
        .timeout(3000)
        .method(Method.GET)
        .execute()

      info = parseHtml(doc.body())


    } catch {

      case ex: SocketTimeoutException => PLogger.warn("time out")
      case ex: ConnectException => ex.printStackTrace()
      case ex: HttpStatusException => changIP()
      case ex: SocketException => ex.printStackTrace()
      case ex: IOException => ex.printStackTrace()

    }

    info

  }

  /**
   * 解析用户信息
   * @param html 有用户信息的str
   * @return 包含用户信息字符串
   */
  def parseHtml(html: String): String = {

    var info = "Nodef"
    var name = "Nodef"
    var phone = "Nodef"
    var email = "Nodef"
    var address = "Nodef"

    try {

      var pattern = Pattern.compile("id=\"customerName.*</span>")
      var m = pattern.matcher(html)

      if (m.find()) {

        val nameArr = m.group().replace("</span>", "").split(">")
        name = nameArr(nameArr.length - 1)

      }

      pattern = Pattern.compile("userEmail\" value=\".*\"")
      m = pattern.matcher(html)

      if (m.find()) {

        val emailArr = m.group().split("\"")
        email = emailArr(2)

      }

      pattern = Pattern.compile("userPhone\" value=\".*\"")
      m = pattern.matcher(html)

      if (m.find()) {

        val phoneArr = m.group().split("\"")
        phone = phoneArr(2)

      }

      pattern = Pattern.compile("selAddressLink.*</a>")
      m = pattern.matcher(html)

      if (m.find()) {

        val addArr = m.group().replace("</a>", "").split(">")
        address = addArr(addArr.length - 1).replace("&nbsp;", "").replace(" ", "").trim

      }
    } catch  {
      case e:Exception =>
    }

    info = name + "-->" + phone + "-->" + email + "-->" + address

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
