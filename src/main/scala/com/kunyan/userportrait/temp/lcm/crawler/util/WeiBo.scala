package com.kunyan.userportrait.temp.lcm.crawler.util

import java.io._
import java.net.{ConnectException, SocketException, SocketTimeoutException}
import java.sql.DriverManager
import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.kunyan.userportrait.temp.lcm.crawler.Controller
import org.jsoup.Connection.Method
import org.jsoup.{HttpStatusException, Jsoup}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Created by lcm on 2016/5/10.
 * 此类用于爬取微博用户的信息
 * 并将信息保存到mysql或者临时空间
 */
object WeiBo {

  var cookies = new ListBuffer[String]

  /**
   *
   * @param data：ua和uid的集合
   */
  def crawlWeiBoInfo(data: ListBuffer[(String, String)], outFile: String): Unit = {

    cookies = getCookies

    //获取微博用户信息
    val weiBoInfo = getWeiBoInfo(data)

    //保存用户信息
    saveWeiBoInfos(weiBoInfo, outFile)


  }

  /**
   * 从配置文件读取cookieStr
   * @return cookieStr
   */
  def getCookies: ListBuffer[String] = {

    val cookies = new ListBuffer[String]
    for (line <- Source.fromFile("/home/liaochengming/crawler/weibo/cookies").getLines()) {

      cookies.+=(line)

    }

    cookies

  }

  /**
   * 将cookie字符串转成map
   * 根据cookie字符串获取cookie的map
   */
  def getCookieMap: util.HashMap[String, String] = {

    val cookieMap = new util.HashMap[String, String]()
    val cookieStr = cookies((Math.random() * cookies.size).toInt)
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
   * @param data ： 用于爬取用户信息的id集合
   * @return：微博的用户信息集合
   */
  def getWeiBoInfo(data: ListBuffer[(String, String)]): HashSet[String] = {

    var weiBoInfo = new HashSet[String]
    var info = ""

    //创建一个可重用固定线程数的线程池
    val pool = Executors.newFixedThreadPool(6)

    for (index <- data.indices) {

      val thread = new Thread(new Runnable {
        override def run(): Unit = {

          //用url获取id
          val id = matchAndGetId(data(index)._1, data(index)._2)

          if (id != "") {

            info = getUserInfoById(id, data(index)._1)

            if (info != "") {

              println("info--" + info)
              weiBoInfo = weiBoInfo.+(info)

            }
          }
        }
      })

      pool.submit(thread)

    }

    pool.shutdown()

    //停止主线程，等到子多线程运行结束再开启
    while (!pool.awaitTermination(3, TimeUnit.SECONDS)) {

    }

    weiBoInfo

  }

  /**
   *
   * @param ua：发送请求所需的参数
   * @param uid：用户的微博id
   * @return：可访问用户信息页面的id
   */
  def matchAndGetId(ua: String, uid: String): String = {

    var id = ""
    try {
      val doc = Jsoup.connect("http://weibo.com/u" + uid)
        .userAgent(ua)
        .timeout(3000)
        .cookies(getCookieMap)
        .method(Method.GET)
        .followRedirects(true)
        .execute()

      doc.body().split("\\$CONFIG").foreach(f => {
        if (f.contains("['page_id']")) {

          id = f.replace(";", "").split("=")(1).replace("'", "").trim

        }
      })
    }
    catch {

      case ex: SocketTimeoutException => Controller.changIP()
      case ex: ConnectException => Controller.changIP()
      case ex: HttpStatusException => Controller.changIP()
      case ex: SocketException => Controller.changIP()
      case ex: IOException =>
        Controller.changIP()
        ex.printStackTrace()
    }

    id

  }

  /**
   * @param id：可访问用户信息页面的id
   * @param ua：请求所需参数
   * @return：用户的微博信息
   */
  def getUserInfoById(id: String, ua: String): String = {

    //用户信息
    var userInfo = ""

    //微博账号
    val weiBoId = id.substring(6)

    //QQ
    var QQ = " "

    //邮箱
    var email = " "

    //职业(不可获得)
    val job = " "

    //身份
    var position = " "

    //真是姓名(不可获得)
    val realName = " "

    //公司
    var company = " "

    //地址（所在地）
    var address = " "

    val url = "http://weibo.com/p/" + id + "/info?mod=pedit_more"

    try {
      val doc = Jsoup.connect(url)
        .userAgent(ua)
        .timeout(3000)
        .cookies(getCookieMap)
        .method(Method.GET)
        .followRedirects(true)
        .execute()

      for (x <- doc.body().split("<script>FM.view")) {
        if (x.contains("\"ns\":\"\",\"domid\":\"Pl_Official_PersonalInfo__62\"")) {

          val data = x.replace("\\t", "").replace("\\n", "").replace("\\r", "")
          val dataArr = data.split("<span class=\\\\\"pt_title S_txt2\\\\\">")
          dataArr.foreach(d => {

            //获取QQ信息
            if (d.contains("QQ")) {

              QQ = parserInfo("QQ", d)
              if (QQ.length > 11) QQ = " "

            }

            //获取邮箱
            if (d.contains("邮箱")) {

              email = parserInfo("邮箱", d)
              if (email.length > 50) email = " "

            }

            //获取所在地
            if (d.contains("所在地")) {

              address = parserInfo("所在地", d)
              if (address.length > 50) address = " "

            }

            //获取公司和身份信息
            if (d.contains("公司")) {

              val workInfo = parserInfo("公司", d).split("=")
              company = workInfo(0)
              if (company.length > 50) company = " "

              if (workInfo.length == 2) position = workInfo(1)

              if (position.length > 20) position = " "

            }
          })
        }
      }

      if (address != " ") {

        userInfo = weiBoId + "-->" + QQ + "-->" + email + "-->" + job + "-->" + position + "-->" + realName + "-->" + company + "-->" + address

      }

    } catch {

      case ex: SocketTimeoutException => Controller.changIP()
      case ex: ConnectException => Controller.changIP()
      case ex: HttpStatusException => Controller.changIP()
      case ex: SocketException => Controller.changIP()
      case ex: IOException =>
        ex.printStackTrace()
        Controller.changIP()
    }

    userInfo

  }

  /**
   *
   * @param infoName：用户的信息名
   * @param infoStr：用于解析的字符串
   * @return：用户信息
   */
  def parserInfo(infoName: String, infoStr: String): String = {

    if (infoName == "公司") {

      val workInfo = infoStr.split("<\\\\/span>")(1)
      val company = workInfo.split("<\\\\/a>")(0).split(">").last
      var position = ""
      if (workInfo.contains("职位")) {

        position = workInfo.split("职位：").last

      }

      company + "=" + position

    } else {

      val anyInfo = infoStr.split("<\\\\/span>")(1).split(">").last
      if (anyInfo.contains("pt_detail")) {

        " "

      } else {

        anyInfo

      }
    }
  }

  /**
   * 用于保存微博信息
   * @param weiBoInfo：微博用于的信息集合
   */
  def saveWeiBoInfos(weiBoInfo: HashSet[String], outFile: String): Unit = {

    val conn_str = "jdbc:mysql://222.73.34.91:3306/personas?user=personas&password=personas"

    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(conn_str)
    val statement = conn.createStatement()
    val wb_weiBoResultSet = statement.executeQuery("SELECT weibo_id FROM weibo")

    val writer = new PrintWriter(outFile)

    //获取微博表中微博id,并保存到集合
    var wbWeiBoIdSet = new HashSet[String]

    while (wb_weiBoResultSet.next()) {

      val weiBoId = wb_weiBoResultSet.getString("weibo_id")
      wbWeiBoIdSet = wbWeiBoIdSet.+(weiBoId)

    }

    val main_weiBoResultSet = statement.executeQuery("SELECT id,weibo FROM main_index where weibo<> \"\"")

    //获取main_index中的微博id,并保存到map
    val mainWeiBoIdMap = new util.HashMap[String, Int]

    while (main_weiBoResultSet.next()) {

      val id = main_weiBoResultSet.getInt("id")
      val weiBo = main_weiBoResultSet.getString("weibo")
      mainWeiBoIdMap.put(weiBo, id)

    }

    for (infoStr <- weiBoInfo) {

      val infoArr = infoStr.split("-->")
      val uid = infoArr(0)
      if (wbWeiBoIdSet.contains(uid)) {

        //将数据保存到临时空间
        writer.write(infoStr + "\n")

      } else {

        if (mainWeiBoIdMap.containsKey(uid)) {

          //将数据写到weiBo表中
          val prep = conn.prepareStatement("INSERT INTO weibo (main_index_id, weibo_id,qq,email,job,position,realName,company,address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ")
          prep.setInt(1, mainWeiBoIdMap.get(uid))
          prep.setString(2, uid)
          prep.setString(3, infoArr(1))
          prep.setString(4, infoArr(2))
          prep.setString(5, infoArr(3))
          prep.setString(6, infoArr(4))
          prep.setString(7, infoArr(5))
          prep.setString(8, infoArr(6))
          prep.setString(9, infoArr(7))
          prep.executeUpdate

        } else {

          //将数据保存到临时空间
          writer.write(infoStr + "\n")

        }
      }
    }

    conn.close()
    writer.close()

  }
}