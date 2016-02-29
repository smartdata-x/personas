package com.kunyan.userportrait.rule.url

import com.kunyan.userportrait.util.StringUtil

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/2/24.
  */
object WeiBo extends  Platform {

  override val PLATFORM_NAME_INFO: String = "WeiBo"
  override val TOP_LEVEL_DOMAIN: String = "%weibo.com%"
  override val PLATFORM_NAME_HTTP: String = "WeiBoHttp"

  //个人信息
  def PersonInformation(url:String):Boolean={
    url.contains("(?<=http://weibo.com/p/")
  }

  def getWeiBoIdFromCookie(cookie:String): String ={
    var weiBoId = ""
    val weiBoIdSet = new mutable.HashSet[String]()
    if (cookie != "NoDef") {
      val result = StringUtil.decodeBase64(cookie)
      // val result = cookie
      val template = "(?<=SUS=SID-)\\d{1,}(?=-)".r
      val resultWeiBoId  =  template.findAllMatchIn(result)
      resultWeiBoId.foreach(x => {
        weiBoId = x.toString()
      })
      if(!"".equals(weiBoId)){
        weiBoIdSet.+=(weiBoId)
      }
      val wbFeedUnfolded = "(?<=wb_feed_unfolded_)\\d{1,}(?==)".r
      val resultWeiBoId2  =  wbFeedUnfolded.findAllMatchIn(result)
      resultWeiBoId2.foreach(x => {
        weiBoId = x.toString()
      })
      if(!"".equals(weiBoId)){
        weiBoIdSet.+=(weiBoId)
      }
    }
    weiBoId = weiBoIdSet.mkString(",").toString
    weiBoId
  }

  def getWeiBoId(line:String): String ={
    val  lineSplit = line.split("\t")
    val cookies = lineSplit(3)
    val cookiesWeiBo = getWeiBoIdFromCookie(cookies)
    cookiesWeiBo
  }

  def extractUrl(url:String):String ={
    if(PersonInformation(url)) url else "NoDef"
  }

  def getWeiBoUrl(line:String):String ={
    val  url = line.split("\t")(2)
    // println(extractUrl(url))
    extractUrl(url)
  }

}
