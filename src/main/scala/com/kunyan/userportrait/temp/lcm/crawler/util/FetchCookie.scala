package com.kunyan.userportrait.temp.lcm.crawler.util

import java.io.{IOException, PrintWriter}
import java.net.{ConnectException, SocketException, SocketTimeoutException, UnknownHostException}
import java.util
import java.util.concurrent.{Executors, TimeUnit}

import org.jsoup.Connection.Method
import org.jsoup.{HttpStatusException, Jsoup}

import scala.collection.immutable.HashSet
import scala.io.Source

/**
 * Created by lcm on 2016/5/18.
 * 此类用于获取当天原始数据的微博cookie
 */
object FetchCookie {


  val UA = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36 QIHU 360SE se.360.cn"

  var IPS: Array[String] = null

  /**
   *
   * @param args：输入文件和输出文件
   *
   */
  def main(args: Array[String]) {

    //切换代理ip
    IPS = getIPs
    changIP()

    //获取需要的cookies
    val cookies = readFiles(args(0))

    //将cookie写出到文件
    outCookies(cookies, args(1))

  }

  /**
   * 将cookie写出到文件
   * @param cookies 要写出的cookie
   * @param outPath 写出的位置
   */
  def outCookies(cookies: HashSet[String], outPath: String): Unit = {

    val writer = new PrintWriter(outPath)

    for (cookie <- cookies) {

      writer.write(cookie + "\n")

    }

    writer.close()
  }

  /**
   * 读数据文件
   * @param fileStr 需要读取数据的文件
   * @return 微博cookie的集合
   */
  def readFiles(fileStr: String): HashSet[String] = {

    var uaAndCookie = new HashSet[(String, String)]

      for (line <- Source.fromFile(fileStr).getLines()) {

        val data = line.split("\t")

        if (data.length > 6) {

          val ua = data(2)
          val url = data(3) + data(4)
          val ref = data(5)
          val cookie = data(6)

          if ((url + ref).contains("weibo.com/p/") || (url + ref).contains("weibo.com/u/")) {

            uaAndCookie = uaAndCookie.+((ua, cookie))

          }
        }
      }

    var cookies = new HashSet[String]

    //创建一个可重用固定线程数的线程池
    val pool = Executors.newFixedThreadPool(6)

    for ((ua, cookie) <- uaAndCookie) {

      val thread = new Thread(new Runnable {
        override def run(): Unit = {

          if (verifyCookie(cookie, ua)) cookies = cookies.+(cookie)

        }
      })

      pool.submit(thread)
    }
    pool.shutdown()

    //停止主线程，等到子多线程运行结束再开启
    while (!pool.awaitTermination(10, TimeUnit.SECONDS)) {}

    cookies
  }

  /**
   * 验证cookie是否能获取用户信息
   * @param cookie 需验证的cookie
   * @param ua 用来设置请求参数的
   * @return 验证cookie是否可返回数据
   */
  def verifyCookie(cookie: String, ua: String): Boolean = {

    var need = true

    try {

      val doc = Jsoup.connect("http://weibo.com/p/1005055495055358/info?mod=pedit_more")
        .userAgent(ua)
        .cookies(getCookieMap(cookie))
        .timeout(3000)
        .method(Method.GET)
        .execute()

      if (!doc.body().contains("Pl_Official_PersonalInfo__62")) need = false

    } catch {

      case ex: SocketTimeoutException =>

        ex.printStackTrace()
        need = false

      case ex: ConnectException =>

        ex.printStackTrace()
        need = false

      case ex: HttpStatusException =>

        ex.printStackTrace()
        changIP()
        need = false

      case ex: SocketException =>

        ex.printStackTrace()
        need = false

      case ex: IOException =>

        ex.printStackTrace()
        need = false

    }

    need

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


  /**
   * 用于获取代理IP
   * @return：IP数组
   */
  def getIPs: Array[String] = {

    var ipPort: Array[String] = null

    try {

      val doc = Jsoup.connect("http://qsdrk.daili666api.com/ip/?tid=558465838696598&num=500&delay=5&foreign=none&ports=80,8080")
        .userAgent(UA)
        .timeout(30000)
        .followRedirects(true)
        .execute()

      ipPort = doc.body().split("\r\n")

    } catch {

      case ex: HttpStatusException => ex.printStackTrace()

      case ex: SocketTimeoutException => ex.printStackTrace()

      case ex: SocketException => ex.printStackTrace()

      case ex: UnknownHostException =>

        ex.printStackTrace()
        changIP()

      case ex: IOException => ex.printStackTrace()

    }

    ipPort
  }


  /**
   * 用来改变代理IP
   */
  def changIP(): Unit = {

    val ipPort = IPS((Math.random() * 200).toInt).split(":")
    val ip = ipPort(0)
    val port = ipPort(1)

    System.getProperties.setProperty("http.proxyHost", ip)
    System.getProperties.setProperty("http.proxyPort", port)

  }
}
