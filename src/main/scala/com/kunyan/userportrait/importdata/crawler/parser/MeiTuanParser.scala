package com.kunyan.userportrait.importdata.crawler.parser

import org.jsoup.Jsoup

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/16.
  * 解析meituan网页数据的主类
  */
object MeiTuanParser {

  /**
    * 解析个人信息页面的数据，获取用户主要信息
    * @param string 网页源数据
    * @return 解析后的结果(个人信息字符串,包含个人信息的map)
    */
  def extractorInfo(string: String): (String, mutable.HashMap[String, String]) = {

    var name = ""
    var address = ""
    var phone = ""
    val  infoMap = new mutable.HashMap[String, String]
    var info = ""
    val html = Jsoup.parse(string)

    try {

      val item = html.body().getElementById("address-table").getElementsByTag("tr")

      for (i <- 0 until item.size) {

        if(i >= 1) {

          val value = item.get(i).getElementsByTag("td")
          name = value.get(0).text()+ "," + name
          address = value.get(1).text() + "," + address
          phone = value.get(2).text() + "," + phone

        }
      }
      if(name.nonEmpty) {
        infoMap.+=(("姓名", name))
      }
      if(address.nonEmpty) {
        infoMap.+=(("地址", address))
      }
      if(phone.nonEmpty) {
        infoMap.+=(("手机号", phone))
      }

    } catch {
      case e:Exception =>
    } finally {

      infoMap.foreach { x =>

        val key = x._1
        val value = x._2

        if(info.nonEmpty) {
          info = info + "\t" + key + ":" + value
        }else {
          info = key + ":"+ value
        }
      }
    }

    (info,infoMap)

  }
}
