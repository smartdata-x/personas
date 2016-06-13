package com.kunyan.userportrait.importdata.crawler.parser

import org.jsoup.Jsoup

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/16.
  * 解析maimai网页数据的主类
  */
object MaiMaiParser {

  /**
    * 解析个人信息页面的数据，获取用户主要信息
    * @param string 网页源数据
    * @return 解析后的结果(个人信息字符串,包含个人信息的map)
    */
  def extractorInfo(string: String): (String, mutable.HashMap[String,String]) = {

    val  infoMap = new mutable.HashMap[String,String]
    var info = ""
    val html = Jsoup.parse(string)

    try {

      val item = html.body().getElementById("body").getElementsByTag("dl")

      for (i <- 0 until item.size) {

        if (i >= 1) {

          val other = item.get(i).getElementsByTag("dd").text().split(" ")
          val size = other.size

          for (j <- 0 until size) {

            val eachItem = other(j).split("：")
            infoMap.+=((eachItem(0), eachItem(1)))

          }

        } else {

          val name = item.get(i).getElementsByTag("dd").get(0).text()
          val position = item.get(i).getElementsByTag("dd").get(1).text()
          val job = item.get(i).getElementsByTag("dd").get(2).text()
          infoMap.+=(("姓名", name))
          infoMap.+=(("职位", position))
          infoMap.+=(("工作", job))
        }
      }
    }catch {
      case e:Exception =>
    }

    try {

        val company = html.body().getElementsByClass("panel-default")

        for(c <- 0 until company.size()) {

          val item  = company.get(c).text().split(" ")
          infoMap.+=((item(0), item(1) +"," + item(2)))

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

  /**
    * 解析获取到的用户列表的id
    * @param bodyData  用户列表的jason字符串
    * @return 返回id的集合
    */
  def extractorMaiMaiUid(bodyData: String): mutable.HashSet[String] = {

    val idSet = new mutable.HashSet[String]()
    val pattern = "(?<=\"id\":)\\d+(?=,\"name\")".r
    val iterator = pattern.findAllMatchIn(bodyData)

    while(iterator.hasNext) {

      val item = iterator.next
      idSet.+=(item.toString)

    }

    idSet

  }

}
