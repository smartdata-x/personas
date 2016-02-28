package com.kunyan.userportrait.rule.url

import com.kunyan.userportrait.util.StringUtil


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
    if (cookie != "NoDef") {
      val result = StringUtil.decodeBase64(cookie)
      val template = "(?<=SUS=SID-)\\d{1,}(?=-)".r
      val resultWeiBoId  =  template.findAllMatchIn(result)
      resultWeiBoId.foreach(x => {
        weiBoId = x.toString()
      })
    }
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
