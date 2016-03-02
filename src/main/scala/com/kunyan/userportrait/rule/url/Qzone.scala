package com.kunyan.userportrait.rule.url

import com.kunyan.userportrait.util.StringUtil

/**
  * Created by C.J.YOU on 2016/2/24.
  */
object Qzone extends  Platform {

  override val TOP_LEVEL_DOMAIN: String = "%.qq.com%"
  override val PLATFORM_NAME_HTTP: String = "QQHttp"
  override val PLATFORM_NAME_INFO: String = "QQ"

  def getQQFromCookies(cookies:String):String = {
    val qqNumberSet = new scala.collection.mutable.HashSet[String]()
    var qqNumber = ""
    if (cookies != "NoDef") {
      val result = StringUtil.decodeBase64(cookies)
      val template = "(?<=qzone_check=)\\d{1,}(?=_)".r
      val resultQQ  =  template.findAllMatchIn(result)
      resultQQ.foreach(x => {
        qqNumber = x.toString()
      })
      if(!"".equals(qqNumber)){
        qqNumberSet.+=(qqNumber)
      }

      val templateOcookie = "(?<=o_cookie=)\\d{1,}(?=)".r
      val ocookieResultQQ  =  templateOcookie.findAllMatchIn(result)
      ocookieResultQQ.foreach(x => {
        qqNumber = x.toString()
      })
      if(!"".equals(qqNumber)) {
        qqNumberSet.+= (qqNumber)
      }
    }
    qqNumber = qqNumberSet.mkString(",").toString
    qqNumber
  }

  def getQQ(line:String): String ={
    val  lineSplit = line.split("\t")
    val cookies = lineSplit(3)
    val cookiesQQ = getQQFromCookies(cookies)
    cookiesQQ
  }

}
