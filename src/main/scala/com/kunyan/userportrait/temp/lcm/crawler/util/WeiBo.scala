package com.kunyan.userportrait.temp.lcm.crawler.util

import java.io.IOException
import java.net.{ConnectException, SocketTimeoutException}
import java.sql.DriverManager
import java.util
import java.util.concurrent.{Executors, TimeUnit}

import com.kunyan.userportrait.temp.lcm.crawler.Controller
import org.jsoup.Connection.Method
import org.jsoup.{HttpStatusException, Jsoup}

import scala.collection.immutable.HashSet

/**
 * Created by lcm on 2016/5/10.
 * 此类用于爬取微博用户的信息
 * 并将信息保存到mysql或者临时空间
 */
object WeiBo {

  var cookieException = 0

  def crawlWeiBoInfo(datas: HashSet[Array[String]]): Unit = {

    val cookieStr = "SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W59Fnqh3XZYLGbqL7yEge5O5JpX5K2hUgL.Fo2RSK.71Kn4SKBt; UOR=,weibo.com,spr_web_360_hao360_weibo_t001; SINAGLOBAL=3532024244608.283.1463017300810; ULV=1463017300844:1:1:1:3532024244608.283.1463017300810:; SUHB=0btNwX-16L0ipb; ALF=1494553349; un=angelxue3427@msn.com; wvr=6; YF-Ugrow-G0=1eba44dbebf62c27ae66e16d40e02964; login_sid_t=b7309f125841ae04196717bebba28d7b; _s_tentry=-; Apache=3532024244608.283.1463017300810; SUS=SID-1859098954-1463017349-GZ-m5r2g-4f451a0dcc87ebac6a48db72a8e1c66a; SUE=es%3Dcb5da348cff7750570aa8d3748dc9d32%26ev%3Dv1%26es2%3D1e2481db867012e9a24bccdc64509bbc%26rs0%3DVDralZgXwwMzfG3fOCwyWuR4aOv3HkMkEhAqLgoLH2SU6XPDVl3Bj6edqCGzJtou2sWfp0FSMouj51Omv7Ga0xLBe%252FgO4W%252BhaGnArRS3lOdsN5rRwb9EzBPvz%252FageDsKCu6eBJwm2tXQkAb08ju0ta33tBR8Y%252FAgUDGAdeWa3%252BU%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1463017349%26et%3D1463103749%26d%3Dc909%26i%3Dc66a%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D0%26st%3D0%26uid%3D1859098954%26name%3Dangelxue3427%2540msn.com%26nick%3D%25E4%25BD%2595%25E6%25AD%25A2%25E6%2583%25B3%25E4%25BD%25A0%26fmp%3D%26lcp%3D2013-05-06%252011%253A02%253A02; SUB=_2A256N6_VDeRxGedG7lsR-SbFzjiIHXVZRIYdrDV8PUNbuNBeLWXRkW9LHesgckLb12l4N_U4-gKymcKRJQ8hgw..; SSOLoginState=1463017349; YF-V5-G0=1913748929273ee181b5c020a6f91640; YF-Page-G0=59104684d5296c124160a1b451efa4ac"

    //获取微博用户信息
    val weiboInfos = getWeiBoInfo(datas, getCookies(cookieStr))

    //保存用户信息
    saveWeiBoInfos(weiboInfos)


  }

  /**
   * 将cookie字符串转成map
   * 根据cookie字符串获取cookie的map
   */
  def getCookies(cookieStr: String): util.HashMap[String, String] = {
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
   * @param datas ： 用于爬取用户信息的id集合
   * @param cookies：用于发送请求带的参数
   * @return：微博的用户信息集合
   */
  def getWeiBoInfo(datas: HashSet[Array[String]], cookies: util.HashMap[String, String]): HashSet[String] = {

    var weiBoInfos = new HashSet[String]

    val listId = datas.toList

    //创建一个可重用固定线程数的线程池

    val pool = Executors.newFixedThreadPool(80);

    for (index <- 0 to listId.size - 1) {

      val thread = new Thread(new Runnable {

        override def run(): Unit = {

          //用url获取id
          val id = matchAndGetId(listId(index)(0), listId(index)(1), cookies)

          if (id != "") {

            val info = getUserInfoById(cookies, id, listId(index)(0))

            if (info != "") {

              println("info--" + info)

              weiBoInfos = weiBoInfos.+(info)

            }

          }

        }

      })

      pool.execute(thread)

    }

    pool.shutdown()

    //停止主线程，等到子多线程运行结束再开启

    while (!pool.awaitTermination(10, TimeUnit.SECONDS)) {

    }

    print("pool over")

    weiBoInfos

  }

  /**
   *
   * @param ua：发送请求所需的参数
   * @param uid：用户的微博id
   * @param cookies：发送请求所需的参数
   * @return：可访问用户信息页面的id
   */
  def matchAndGetId(ua: String, uid: String, cookies: util.HashMap[String, String]): String = {

    var id = ""

    try {

      val doc = Jsoup.connect("http://weibo.com/u" + uid)
        .userAgent(ua)
        .timeout(3000)
        .cookies(cookies)
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

      case ex: SocketTimeoutException => {

        matchAndGetId(ua, uid, cookies)

      }

      case ex: ConnectException => println(ex)

      case ex: HttpStatusException => {

        println(ex)

        Controller.changIP()

      }

      case ex: IOException => println(ex)

    }

    id

  }

  /**
   *
   * @param cookies：请求所需参数
   * @param id：可访问用户信息页面的id
   * @param ua：请求所需参数
   * @return：用户的微博信息
   */
  def getUserInfoById(cookies: util.HashMap[String, String], id: String, ua: String): String = {

    //用户信息
    var userInfo = ""

    //微博账号
    val weiBoId = id.substring(6)

    //QQ
    var QQ = "NoDef"

    //邮箱
    var email = "NoDef"

    //职业(不可获得)
    val job = "NoDef"

    //身份
    var position = "NoDef"

    //真是姓名(不可获得)
    val realName = "NoDef"

    //公司
    var company = "NoDef"

    //地址（所在地）
    var address = "NoDef"

    //var
    val url = "http://weibo.com/p/" + id + "/info?mod=pedit_more"

    try {
      val doc = Jsoup.connect(url)
        .userAgent(ua)
        .timeout(3000)
        .cookies(cookies)
        .method(Method.GET)
        .followRedirects(true)
        .execute()

      for (x <- doc.body().split("<script>FM.view")) {

        if (x.contains("\"ns\":\"\",\"domid\":\"Pl_Official_PersonalInfo__62\"")) {

          val data = x.replace("\\t", "").replace("\\n", "").replace("\\r", "")

          val datas = data.split("<span class=\\\\\"pt_title S_txt2\\\\\">")

          datas.foreach(d => {

            //获取QQ信息
            if (d.contains("QQ")) {

              QQ = parserInfo("QQ", d)

            }

            //获取邮箱
            if (d.contains("邮箱")) {

              email = parserInfo("邮箱", d)

            }

            //获取所在地

            if (d.contains("所在地")) {

              address = parserInfo("所在地", d)

            }

            //获取公司和身份信息
            if (d.contains("公司")) {

              val workInfo = parserInfo("公司", d).split("=")

              company = workInfo(0)

              if (workInfo.length == 2) {

                position = workInfo(1)

              }

            }

          })

        }

      }

      if (address != "NoDef") {

        userInfo = weiBoId + "-->" + QQ + "-->" + email + "-->" + job + "-->" + position + "-->" + realName + "-->" + company + "-->" + address

      }

    } catch {

      case ex: SocketTimeoutException => getUserInfoById(cookies, id, ua)

      case ex: ConnectException => println(ex)

      case ex: HttpStatusException => {

        println(ex)

        Controller.changIP()

      }

      case ex: IOException => println(ex)

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

      return company + "=" + position

    } else {

      val anyInfo = infoStr.split("<\\\\/span>")(1).split(">").last

      if (anyInfo.contains("pt_detail")) {

        return "NoDef"

      } else {

        return anyInfo

      }

    }

  }

  /**
   * 用于保存微博信息
   * @param weiboInfos：微博用于的信息集合
   */
  def saveWeiBoInfos(weiboInfos: HashSet[String]): Unit = {

    val conn_str = "jdbc:mysql://222.73.34.91:3306/personas?user=personas&password=personas"

    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(conn_str)

    val statement = conn.createStatement()

    val wb_weiboResultSet = statement.executeQuery("SELECT weibo_id FROM weibo")

    //获取微博表中微博id,并保存到集合
    var wbWeiboIdSet = new HashSet[String]

    while (wb_weiboResultSet.next()) {

      val weiboId = wb_weiboResultSet.getString("weibo_id")

      wbWeiboIdSet = wbWeiboIdSet.+(weiboId)

      println("weibo-id =" + weiboId)

    }

    val main_weiboResultSet = statement.executeQuery("SELECT id,weibo FROM main_index where weibo<> \"\"")

    //获取main_index中的微博id,并保存到map
    val mainWeiboIdMap = new util.HashMap[String, Int]

    while (main_weiboResultSet.next()) {

      val id = main_weiboResultSet.getInt("id")

      val weibo = main_weiboResultSet.getString("weibo")

      mainWeiboIdMap.put(weibo, id)

    }
    println(mainWeiboIdMap.size)

    for (infoStr <- weiboInfos) {

      val infos = infoStr.split("-->")

      val uid = infos(0)

      if (wbWeiboIdSet.contains(uid)) {

        //将数据保存到临时空间

      } else {

        if (mainWeiboIdMap.containsKey(uid)) {

          //将数据写到weibo表中

          val prep = conn.prepareStatement("INSERT INTO weibo (main_index_id, weibo_id,qq,email,job,position,realName,company,address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ")
          prep.setInt(1, mainWeiboIdMap.get(uid))
          prep.setString(2, uid)
          prep.setString(3, infos(1))
          prep.setString(4, infos(2))
          prep.setString(5, infos(3))
          prep.setString(6, infos(4))
          prep.setString(7, infos(5))
          prep.setString(8, infos(6))
          prep.setString(9, infos(7))
          prep.executeUpdate

        } else {

          //将数据保存到临时空间

        }
      }
    }

    conn.close()
  }

}
