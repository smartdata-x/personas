package com.kunyan.userportrait.importdata.crawler.parser

import org.jsoup.Jsoup

/**
  * Created by C.J.YOU on 2016/5/16.
  * 解析maimai网页数据的主类
  */
object MaimaiParser {

  def extractorInfo(string: String): String = {

    var info = ""
    val html = Jsoup.parse(string)
    val item = html.body().getElementsByClass("list-group-item")

    for(i <- 0 until item.size) {

      if(i >= 1) {

        val maimaiNumber = item.get(i).getElementsByTag("dd").get(0).text()//.split("脉脉号：")(1)
        val phone = item.get(i).getElementsByTag("dd").get(1).text()//.split("手机号：")(1)
        val email = item.get(i).getElementsByTag("dd").get(2).text()//.split("邮箱：")(1)
        val location = item.get(i).getElementsByTag("dd").get(3).text()//.split("地址：")(1)

        info = info + "\t" + maimaiNumber + "\t" + phone + "\t" + email + "\t" + location

      }else {

        val name = item.get(i).getElementsByTag("dd").get(0).text()
        val position = item.get(i).getElementsByTag("dd").get(1).text()
        val job = item.get(i).getElementsByTag("dd").get(2).text()

        info = name + "\t" + position + "\t" + job

      }
    }

    info
  }


}
